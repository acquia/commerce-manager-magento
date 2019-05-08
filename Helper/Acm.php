<?php

namespace Acquia\CommerceManager\Helper;

use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Catalog\Api\Data\ProductInterface;
use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Acquia\CommerceManager\Helper\Stock as StockHelper;

/**
 * Class Acm
 * @package Acquia\CommerceManager\Helper
 *
 * Exposes general methods for adding ACM specific data into the API responses
 *
 */
class Acm extends AbstractHelper
{
    /**
     * @var \Magento\Framework\Webapi\ServiceOutputProcessor $serviceOutputProcessor
     */
    private $serviceOutputProcessor;

    /**
     * @var \Magento\Catalog\Model\ResourceModel\Product\Gallery $galleryResource
     */
    private $galleryResource;

    /**
     * @var \Magento\Catalog\Model\Product\Gallery\ReadHandler
     * @since 100.1.0
     */
    private $mediaGalleryReadHandler;

    /**
     * @var \Acquia\CommerceManager\Model\Product\RelationBuilderInterface $relationBuilder;
     */
    private $relationBuilder;

    /**
     * @var \Magento\Eav\Api\AttributeSetRepositoryInterface $attributeSet
     */
    private $attributeSet;

    /**
     * The array of data that eventually gets sent out in the API response
     * @var array $record
     */
    private $record;

    /**
     * The product used to generate the 'record'
     * @var ProductInterface $product
     */
    private $product;

    /**
     * @var integer $mediaGalleryAttributeId
     */
    private $mediaGalleryAttributeId;

    /**
     * @var ClientHelper $clientHelper
     */
    private $clientHelper;

    /**
     * @var \Magento\Store\Model\StoreManagerInterface $storeManager
     */
    private $storeManager;

    /**
     * @var StockHelper $stockHelper
     */
    private $stockHelper;

    /**
     * @var \Magento\Framework\App\ResourceConnection $resource
     */
    private $resource;

    /**
     * @var \Magento\Framework\EntityManager\MetadataPool
     */
    private $metadataPool;

    /**
     * Acm constructor.
     * @param \Magento\Framework\EntityManager\MetadataPool $metadataPool
     * @param \Magento\Store\Model\StoreManagerInterface $storeManager
     * @param ClientHelper $clientHelper
     * @param StockHelper $stockHelper
     * @param \Magento\Framework\Webapi\ServiceOutputProcessor $serviceOutputProcessor
     * @param \Magento\Catalog\Model\ResourceModel\Product\Gallery $galleryResource
     * @param \Magento\Catalog\Model\Product\Gallery\ReadHandler $mediaGalleryReadHandler
     * @param \Acquia\CommerceManager\Model\Product\RelationBuilderInterface $relationBuilder
     * @param \Magento\Eav\Api\AttributeSetRepositoryInterface $attributeSet
     * @param \Magento\Framework\App\Helper\Context $context
     * @param \Magento\Framework\App\ResourceConnection $resource
     */
    public function __construct(
        \Magento\Framework\EntityManager\MetadataPool $metadataPool,
        \Magento\Store\Model\StoreManagerInterface $storeManager,
        ClientHelper $clientHelper,
        StockHelper $stockHelper,
        \Magento\Framework\Webapi\ServiceOutputProcessor $serviceOutputProcessor,
        \Magento\Catalog\Model\ResourceModel\Product\Gallery $galleryResource,
        \Magento\Catalog\Model\Product\Gallery\ReadHandler $mediaGalleryReadHandler,
        \Acquia\CommerceManager\Model\Product\RelationBuilderInterface $relationBuilder,
        \Magento\Eav\Api\AttributeSetRepositoryInterface $attributeSet,
        \Magento\Framework\App\Helper\Context $context,
        \Magento\Framework\App\ResourceConnection $resource
    ) {
        $this->metadataPool = $metadataPool;
        $this->stockHelper = $stockHelper;
        $this->storeManager = $storeManager;
        $this->clientHelper = $clientHelper;
        $this->serviceOutputProcessor = $serviceOutputProcessor;
        $this->galleryResource = $galleryResource;
        $this->mediaGalleryReadHandler = $mediaGalleryReadHandler;
        $this->relationBuilder = $relationBuilder;
        $this->attributeSet = $attributeSet;
        $this->resource = $resource;

        parent::__construct($context);
    }

    /**
     * getProductDataForAPI
     * Creates the API data record array ready for sending to the middleware
     * and adds in additional product attributes for sending along to the ACM connector
     * You may want to plugin around or after this with any customisations
     *
     * @param ProductInterface $product
     * @return array
     * @internal param array $record
     */
    public function getProductDataForAPI(ProductInterface $product)
    {
        $this->record = [];
        $this->product = $product;
        $this->_getProductDataForAPI();
        return $this->record;
    }

    /**
     * private version of public getProductDataForAPI
     *
     */
    private function _getProductDataForAPI()
    {
        // 1. Add the stock data to the product object.
        $this->stockAttributes();

        // 2. Build the Magento standard API product data (GET) array.
        $this->record = $this->serviceOutputProcessor->process(
            $this->product,
            \Magento\Catalog\Api\ProductRepositoryInterface::class,
            'get'
        );

        // 3. Add the additional ACM attributes into the API data array
        $this->addPricesToRecord();
        $this->record['store_id'] = (integer) $this->product->getStoreId();
        $this->record['categories'] = $this->product->getCategoryIds();
        $this->record['attribute_set_id'] = (integer) $this->product->getAttributeSetId();
        $attributeSetRepository = $this->attributeSet->get($this->product->getAttributeSetId());
        $this->record['attribute_set_label'] = $attributeSetRepository->getAttributeSetName();

        if (!array_key_exists('extension_attributes', $this->record)) {
            $this->record['extension_attributes'] = [];
        }

        // Later (Malachy): Please use acm or acquia namespace inside 'extension attributes'
        $this->record['extension_attributes'] = array_merge(
            $this->record['extension_attributes'],
            $this->relationBuilder->generateRelations($this->product),
            $this->processMediaGalleryExtension()
        );
    }

    /**
     * addPricesToRecord
     *
     * Adds the prices into the record.
     * May be customer specific. Consider using a plugin for customer
     * specific implementations.
     */
    public function addPricesToRecord()
    {
        $this->record['price'] = (string) $this->product->getPrice();

        // Later (Malachy): getPrice should not return empty
        // If it is empty there is a misconfig in the way the product
        // was loaded or, more likely, getPrice() is misunderstood.
        // Only two prices matter: regular_price and final_price.
        // Product->getPrice() only retrieves the base price from the database.
        // You rarely ever want product->getPrice().
        if (empty($this->record['price'])) {
            $this->record['price'] = (string) $this->product->getPriceInfo()->getPrice('final_price')->getValue();
        }

        // Later (Malachy): Are you sure you want to get the special price like this?
        // All it does is return the field stored in the database.
        $this->record['special_price'] = (string) $this->product->getSpecialPrice();

        // These are the only two prices that matter.
        $this->record['regular_price'] = (string) $this->product->getPriceInfo()->getPrice('regular_price')->getValue();
        // TODO (malachy): review the need for ->getMinimalPrice(). Is it harmless?
        $this->record['final_price'] = (string) $this->product->getPriceInfo()->getPrice('final_price')->getMinimalPrice()->getValue();

        // TODO (malachy): is this a good fallback? Can it ever happen? Is generating an exception a more robust solution?
        // Fallback.
        if (empty($this->record['final_price'])) {
            $this->record['final_price'] = $this->product->getFinalPrice();
        }
    }

    /**
     * stockAttributes
     *
     * Add stock info to the product.
     *
     * @return ProductInterface
     */
    public function stockAttributes()
    {
        $storeId = $this->product->getStoreId();

        // Adding stock info to the product.
        $scopeId = $this->storeManager->getStore($storeId)->getWebsiteId();
        $stockItem = $this->stockHelper->getStockInfo($this->product->getId(), $scopeId, TRUE);

        $productExtension = $this->product->getExtensionAttributes();
        $productExtension->setStockItem($stockItem);
        $this->product->setExtensionAttributes($productExtension);
    }

    /**
     * processMediaGalleryExtension
     *
     * Add full media gallery items information to product extension data.
     *
     * @return array $mediaItems
     */
    private function processMediaGalleryExtension()
    {
        if (empty($this->mediaGalleryAttributeId)) {
            $this->mediaGalleryAttributeId = $this->mediaGalleryReadHandler->getAttribute()->getId();
        }

        $gallery = [
            'images' => [],
            'values' => [],
        ];

        $mediaEntries = $this->galleryResource->loadProductGalleryByAttributeId(
            $this->product,
            $this->mediaGalleryAttributeId
        );

        // We should add media roles in response.
        // Using roles list from magento/module-elasticsearch/Model/Adapter/DataMapper/ProductDataMapper.php class.
        $attributes['image'] = $this->product->getImage();
        $attributes['small_image'] = $this->product->getSmallImage();
        $attributes['base_image'] = $this->product->getBaseImage();
        $attributes['swatch_image'] = $this->product->getSwatchImage();
        $attributes['thumbnail'] = $this->product->getThumbnail();

        foreach ($mediaEntries as $mediaEntry) {
            $filterEntry = [];
            foreach ($mediaEntry as $key => $rawValue) {
                if (null !== $rawValue) {
                    $processedValue = $rawValue;
                } elseif (isset($mediaEntry[$key . '_default'])) {
                    $processedValue = $mediaEntry[$key . '_default'];
                } else {
                    $processedValue = null;
                }
                $filterEntry[$key] = $processedValue;
            }

            if (isset($filterEntry['file'])) {
                $filterEntry['roles'] = [];
                foreach ($attributes as $role => $file) {
                    if ($filterEntry['file'] == $file) {
                        $filterEntry['roles'][] = $role;
                    }
                }

                $filterEntry['file'] =
                    $this->product->getMediaConfig()->getMediaUrl($filterEntry['file']);
            }

            $gallery['images'][$mediaEntry['value_id']] = $filterEntry;
        }

        return ([
            'media' => $this->serviceOutputProcessor->process(
                $gallery,
                \Magento\Catalog\Api\ProductAttributeMediaGalleryManagementInterface::class,
                'getList'
            )
        ]);
    }

    /**
     * Wrapper function to get all active store ids.
     *
     * @return array
     *   Store ids.
     */
    public function getAllActiveStoreIds() {
        static $store_ids = [];

        if (!empty($store_ids)) {
            return $store_ids;
        }

        foreach ($this->storeManager->getStores() as $store) {
            // Avoid default store id 0.
            if ($store->getId()) {
                $store_ids[$store->getCode()] = $store->getId();
            }
        }

        return $store_ids;
    }

    /**
     * Wrapper function to get all stores for particular website.
     *
     * @param string $website_code
     *   Website code.
     *
     * @return array
     *   Store ids in the website.
     */
    public function getAllStoresForWebsite($website_code) {
        static $store_ids = [];

        if (!empty($store_ids[$website_code])) {
            return $store_ids[$website_code];
        }

        /** @var \Magento\Store\Model\Website $website */
        foreach ($this->storeManager->getWebsites(TRUE) as $website) {
            $store_ids[$website->getCode()] = $website->getStoreIds();
        }

        return $store_ids[$website_code];
    }

    /**
     * Wrapper function to get all stores in website of thestore.
     *
     * @param string $store_code
     *   Store code.
     *
     * @return array
     *   Store ids in the website of the store.
     */
    public function getAllStoresInWebsiteForStore($store_code) {
        static $store_ids = [];

        if (!empty($store_ids[$store_code])) {
            return $store_ids[$store_code];
        }

        /** @var \Magento\Store\Model\Website $website */
        foreach ($this->storeManager->getWebsites(TRUE) as $website) {
            /** @var \Magento\Store\Model\Store $store */
            foreach ($website->getStores() as $store) {
                $store_ids[$store->getCode()] = $website->getStoreIds();
            }
        }

        return $store_ids[$store_code];
    }

    /**
     * Helper function to get status for SKUs in specific stores in one go.
     *
     * @param array $skus
     *   SKUs to get the status for.
     * @param array $store_ids
     *   Store IDs to get the status for.
     *
     * @return array
     *   Multi-dimensional array.
     *   With sku as key for first dimension, store_id as key for second, status as value.
     */
    public function getProductStatusForStores(array $skus, $store_ids = [])
    {
        // Fetch list of all enabled stores.
        if (empty($store_ids) || !is_array($store_ids)) {
            $store_ids = $this->getAllActiveStoreIds();
        }

        /** @var \Magento\Framework\DB\Adapter\AdapterInterface $readConnection */
        $readConnection = $this->resource->getConnection('core_read');

        $metadata = $this->metadataPool->getMetadata(ProductInterface::class);
        $linkField = $metadata->getLinkField();

        // Get list of all stores the product has an entry for. We are
        // not filtering the results based on enabled/disabled status
        // here, since we would never know from DB directly if a product
        // was using value from default value from store or its an
        // overridden one. Magento doesn't store this info explicitly,
        // instead avoids rows in DB for stores using default value &
        // have only overrides in the table.
        $sql = $readConnection->select()->from(['cpei' => 'catalog_product_entity_int'], ['store_id', 'value'])
            ->join(['ea' => 'eav_attribute'], 'ea.attribute_id = cpei.attribute_id AND ea.attribute_code="status"')
            ->join(['cpe' => 'catalog_product_entity'], 'cpe.'.$linkField.' = cpei.'.$linkField, ['cpe.sku'])
            ->where('cpe.sku IN (?)', $skus)
            ->where('store_id = 0 OR store_id IN (?)', $store_ids);

        $results = $readConnection->fetchAll($sql);

        $statuses = [];
        foreach ($results as $result) {
            if ($result['store_id'] == 0) {
                // Add to $statuses only if not already added.
                // If found later in $result, it will be overridden in else.
                foreach ($store_ids as $storeId) {
                    if (!isset($statuses[$result['sku']][$storeId])) {
                        $statuses[$result['sku']][$storeId] = $result['value'];
                    }
                }
            }
            else {
                $statuses[$result['sku']][$result['store_id']] = $result['value'];
            }
        }

        return $statuses;
    }
}
