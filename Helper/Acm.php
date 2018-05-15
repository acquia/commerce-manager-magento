<?php

namespace Acquia\CommerceManager\Helper;

use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Catalog\Api\Data\ProductInterface;
use Acquia\CommerceManager\Helper\Data as ClientHelper;

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
     * @var \Magento\CatalogInventory\Api\StockRegistryInterface $stockRegistry
     */
    private $stockRegistry;

    /**
     * Acm constructor.
     * @param \Magento\Framework\Webapi\ServiceOutputProcessor $serviceOutputProcessor
     * @param \Magento\Catalog\Model\ResourceModel\Product\Gallery $galleryResource
     * @param \Magento\Catalog\Model\Product\Gallery\ReadHandler $mediaGalleryReadHandler
     * @param \Acquia\CommerceManager\Model\Product\RelationBuilderInterface $relationBuilder
     * @param \Magento\Eav\Api\AttributeSetRepositoryInterface $attributeSet
     * @param \Magento\Framework\App\Helper\Context $context
     */
    public function __construct(
        \Magento\CatalogInventory\Api\StockRegistryInterface $stockRegistry,
        \Magento\Store\Model\StoreManagerInterface $storeManager,
        ClientHelper $clientHelper,
        \Magento\Framework\Webapi\ServiceOutputProcessor $serviceOutputProcessor,
        \Magento\Catalog\Model\ResourceModel\Product\Gallery $galleryResource,
        \Magento\Catalog\Model\Product\Gallery\ReadHandler $mediaGalleryReadHandler,
        \Acquia\CommerceManager\Model\Product\RelationBuilderInterface $relationBuilder,
        \Magento\Eav\Api\AttributeSetRepositoryInterface $attributeSet,
        \Magento\Framework\App\Helper\Context $context
    ) {
        $this->stockRegistry = $stockRegistry;
        $this->storeManager = $storeManager;
        $this->clientHelper = $clientHelper;
        $this->serviceOutputProcessor = $serviceOutputProcessor;
        $this->galleryResource = $galleryResource;
        $this->mediaGalleryReadHandler = $mediaGalleryReadHandler;
        $this->relationBuilder = $relationBuilder;
        $this->attributeSet = $attributeSet;

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
        $this->record['final_price'] = (string) $this->product->getPriceInfo()->getPrice('final_price')->getMinimalPrice()->getValue();

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
        $store_id = $this->product->getStoreId();
        $scopeId = $this->storeManager->getStore($store_id)->getWebsiteId();
        $stock_item = $this->stockRegistry->getStockItem(
            $this->product->getId(), $scopeId
        );
        $productExtension = $this->product->getExtensionAttributes();
        $productExtension->setStockItem($stock_item);
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
}
