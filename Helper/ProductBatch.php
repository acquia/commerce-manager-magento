<?php

/**
 * Acquia/CommerceManager/Helper/ProductBatch.php
 *
 * Acquia Commerce Product Batch Helper
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Helper;

use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Model\Cache\Type\Acm as AcmCache;
use Magento\Catalog\Api\Data\ProductInterface;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Framework\App\Helper\Context;
use Magento\Framework\Module\ModuleListInterface;
use Psr\Log\LoggerInterface;
use Magento\Catalog\Model\Product\Attribute\Source\Status;

/**
 * ProductBatch
 *
 * Acquia Commerce Product Helper
 */
class ProductBatch extends AbstractHelper
{
    /**
     * Connector Product Update Endpoint
     *
     * @const ENDPOINT_PRODUCT_UPDATE
     */
    const ENDPOINT_PRODUCT_UPDATE = 'ingest/product';

    /**
     * Consumer name (see etc/communications.xml).
     */
    const PRODUCT_PUSH_CONSUMER = 'connector.product.push';

    /**
     * Magento Message Queue Module Name
     * @const MESSAGEQUEUE_MODULE
     */
    const MESSAGEQUEUE_MODULE = 'Magento_MessageQueue';

    /**
     * Acquia Commerce Manager Client Helper
     *
     * @var ClientHelper $clientHelper
     */
    private $clientHelper;

    /**
     * Acquia Commerce Manager ACM Helper
     *
     * @var AcmHelper $clientHelper
     */
    private $acmHelper;

    /**
     * System Logger
     *
     * @var LoggerInterface $logger
     */
    protected $logger;

    /**
     * \Magento\Framework\MessageQueue\PublisherInterface
     * EE only
     */
    private $publisher;

    /**
     * @var ProductRepositoryInterface $productRepository
     */
    private $productRepository;

    /**
     * Magento Module List Service
     * @var ModuleListInterface $moduleList
     */
    private $moduleList;

    /**
     * ACM Cache.
     *
     * @var AcmCache
     */
    private $cache;

    /**
     * ProductBatch constructor.
     *
     * @param Context $context
     * @param ProductRepositoryInterface $productRepository
     * @param Data $clientHelper
     * @param AcmHelper $acmHelper
     * @param ModuleListInterface $moduleList
     * @param AcmCache $cache
     */
    public function __construct(
        Context $context,
        ProductRepositoryInterface $productRepository,
        ClientHelper $clientHelper,
        AcmHelper $acmHelper,
        ModuleListInterface $moduleList,
        AcmCache $cache
    ) {
        $this->productRepository = $productRepository;
        $this->clientHelper = $clientHelper;
        $this->acmHelper = $acmHelper;
        $this->moduleList = $moduleList;
        $this->logger = $context->getLogger();
        $this->cache = $cache;
        parent::__construct($context);
    }


    /**
     * {@inheritDoc}
     *
     * @return bool $enabled
     */
    public function getMessageQueueEnabled()
    {
        return ($this->_getMessageQueueEnabled());
    }

    /**
     * targetModuleEnabled
     *
     * Check if the Magento EE Magento\Framework\MessageQueue\PublisherInterface class is installed / enabled.
     *
     * @return bool $enabled
     */
    private function _getMessageQueueEnabled()
    {
        return ($this->moduleList->has(self::MESSAGEQUEUE_MODULE));
    }

    /**
     * getMessageQueue
     *
     * Build a MessageQueue\Publisher model from the ObjectManager to prevent
     * coupling to EE.
     *
     * @return null|\Magento\Framework\MessageQueue\Publisher
     */
    private function getMessageQueue()
    {
        if(!$this->publisher) {
            if ($this->getMessageQueueEnabled()) {
                // Object Manager's get() is type-preference aware,
                // so we can request a class using its interface
                $this->publisher = \Magento\Framework\App\ObjectManager::getInstance()->create(
                    \Magento\Framework\MessageQueue\PublisherInterface::class
                );
            } else {
                $this->publisher = null;
                // Or perhaps use a class that mimics the queue using Magento CRON
            }
        }

        return ($this->publisher);
    }


    /**
     * pushProduct.
     *
     * Helper function to push product to front-end.
     *
     * @param ProductInterface $product
     * @param string           $action
     */
    public function pushProduct(ProductInterface $product, $action = 'productSave')
    {
        $storeId = $product->getStoreId();

        // Load again using getById to ensure every-thing is loaded.
        $product = $this->productRepository->getById(
            $product->getId(),
            false,
            $storeId
        );

        $productDataByStore[$storeId][] = $this->acmHelper->getProductDataForAPI($product);

        $this->pushMultipleProducts($productDataByStore, $action);
    }

    /**
     * pushProduct.
     *
     * Helper function to push product to front-end.
     *
     * @param array $productDataByStore
     * @param string $action
     */
    public function pushMultipleProducts($productDataByStore, $action = 'productSave' ) {

        // We need to have separate requests per store so we can assign them
        // correctly in middleware.
        foreach ($productDataByStore as $storeId => $arrayOfProducts) {

            // Send Connector request.
            $doReq = function ($client, $opt) use ($arrayOfProducts) {
                $opt['json'] = $arrayOfProducts;
                return $client->post(self::ENDPOINT_PRODUCT_UPDATE, $opt);
            };

            $this->clientHelper->tryRequest($doReq, $action, $storeId);
        }
    }

    /**
     * Add batch (consisting of array of sku and store_id) to queue.
     *
     * @param mixed $batch
     *   Array of arrays containing sku and store_id.
     * @param string $caller
     *   Calling function to add to logs.
     */
    public function addBatchToQueue($batch, $caller)
    {
        $batch = array_filter(array_map(function($batchItem) {
            try {
                if ($this->isProductPushReduceDuplicatesEnabled()) {
                    $this->markProductAsQueuedForPushing($batchItem);
                }

                return $batchItem;
            }
            catch (\OutOfBoundsException $e) {
                // Do nothing, we will return NULL for this.
            }

            return NULL;
        }, $batch));

        if (!empty($batch)) {
            // MessageQueue is EE only. So do nothing if there is no queue.
            $messageQueue = $this->getMessageQueue();
            if ($messageQueue) {
                $batch = json_encode($batch);
                $this->publisher->publish(self::PRODUCT_PUSH_CONSUMER, $batch);

                $this->_logger->info('Added products to queue for pushing in background.', [
                    'caller' => $caller,
                    'data' => $batch,
                ]);
            } else {
                // At 20180123, do nothing.
                // Later (Malachy or Anuj): Use Magento CRON instead
            }
        }
    }

    /**
     * Function to process products in batches and add to queue.
     *
     * @param array $products
     *   Array of arrays containing sku/product_id and stores.
     * @param $caller
     *   Identifier calling this function for logs.
     * @param bool $check_status
     *   Flag to specify if status should be checked or not.
     */
    public function addProductsToQueue(array $products, $caller, $check_status = true)
    {
        // First normalise data.
        // We want sku for all.
        $products = $this->replaceProductIdWithSku($products);

        $productsToQueue = [];

        // We want store_id and one row per store.
        foreach ($products as $product) {
            if (isset($product['stores'])) {
                $product['stores'] = is_array($product['stores']) ? $product['stores'] : [$product['stores']];
                foreach ($product['stores'] as $storeId) {
                    $productsToQueue[$storeId][$product['sku']] = [
                        'sku' => $product['sku'],
                        'store_id' => $storeId,
                    ];
                }
            }
            // Send to all the stores.
            else {
                $productsToQueue[0][$product['sku']] = [
                    'sku' => $product['sku'],
                    'store_id' => 0,
                ];
            }
        }

        // Remove disabled stores.
        if ($check_status) {
            $skus = array_column($products, 'sku');
            $statuses = $this->acmHelper->getProductStatusForStores($skus);

            foreach ($productsToQueue as $storeId => $storeProducts) {
                // For store id 0 or when pushing to all the stores,
                // we will check for status when pushing to ACM.
                if (empty($storeId)) {
                    continue;
                }

                foreach ($storeProducts as $sku => $product) {
                    if ($statuses[$sku][$storeId] == Status::STATUS_DISABLED) {
                        unset($productsToQueue[$storeId][$sku]);
                    }
                }
            }
        }

        // Queue in chunks.
        $batchSize = $this->getProductQueueBatchSize();
        foreach ($productsToQueue as $storeId => $storeProducts) {
            foreach (array_chunk($storeProducts, $batchSize) as $products) {
                $this->addBatchToQueue($products, $caller);
            }
        }
    }

    /**
     * Get batch size from config.
     *
     * @return mixed
     */
    public function getProductPushBatchSize()
    {
        $path = 'webapi/acquia_commerce_settings/product_push_batch_size';

        $batchSize = (int) $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );

        // Use 5 by default.
        $batchSize = $batchSize ?? 5;

        return $batchSize;
    }

    /**
     * Get batch size from config to use for queue.
     *
     * @return mixed
     */
    public function getProductQueueBatchSize()
    {
        $path = 'webapi/acquia_commerce_settings/product_queue_batch_size';

        $batchSize = $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );

        return $batchSize;
    }

    /**
     * Check if we need to push product on attribute changes.
     *
     * @return bool
     */
    public function pushOnProductAttributeUpdate() {
        $path = 'webapi/acquia_commerce_settings/push_on_attribute_update';

        return (bool) $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );
    }

    /**
     * Check if we need to reduce duplicates for products pushed via queue.
     *
     * @return bool
     */
    public function isProductPushReduceDuplicatesEnabled()
    {
        $path = 'webapi/acquia_commerce_settings/product_push_reduce_duplicates';

        return (bool) $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );
    }

    /**
     * Get product_push_queue_lock_expire_time config value.
     *
     * @return int
     */
    public function getProductPushQueueLockExpireTime()
    {
        $path = 'webapi/acquia_commerce_settings/product_push_queue_lock_expire_time';

        return (int) $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );
    }

    /**
     * Mark product as queued for pushing, add cache entry.
     *
     * @param array $batchItem
     *   [product_id, store_id] or [sku, store_id]
     *
     * @throws \Exception
     *   When product is already queued.
     */
    public function markProductAsQueuedForPushing(array $batchItem)
    {
        // First check if product is queued for all stores.
        $allStoresCheck = $batchItem;
        $allStoresCheck['store_id'] = null;
        $allStoresCheckCacheId = $this->getProductPushBatchCacheId($allStoresCheck);
        $data = $this->cache->load($allStoresCheckCacheId);
        if (!empty($data)) {
            throw new \OutOfBoundsException('Item already in queue.');
        }

        $cacheId = $this->getProductPushBatchCacheId($batchItem);
        // Check again if there is store_id in request.
        if (!empty($batchItem['store_id'])) {
            $data = $this->cache->load($cacheId);
            if (!empty($data)) {
                throw new \OutOfBoundsException('Item already in queue.');
            }
        }

        $this->cache->save($cacheId, $cacheId, [], $this->getProductPushQueueLockExpireTime());
    }

    /**
     * Set product as pushed, remove cache entry.
     *
     * @param array $batchItem
     *   [product_id, store_id] or [sku, store_id]
     */
    public function setProductAsPushedFromQueue(array $batchItem)
    {
        $cacheId = $this->getProductPushBatchCacheId($batchItem);
        $this->cache->remove($cacheId);
    }

    /**
     * Get product push batch item cache id.
     *
     * @param array $batchItem
     *   [product_id, store_id] or [sku, store_id]
     *
     * @return string
     *   Cache ID for batch item.
     */
    private function getProductPushBatchCacheId(array $batchItem): string
    {
        $cache_id = 'acm:';
        $cache_id .= implode('|', array_keys($batchItem)) . ':';
        $cache_id .= implode('|', $batchItem);

        return $cache_id;
    }

    /**
     * assignProperSkus.
     *
     * Assign proper SKUs to each row in batch. For configurable products we
     * usually get two entries for both parent and child products but with same
     * (child) SKUs in both. This blocks our flow as we rely heavily on SKUs.
     *
     * @param array $batch
     *
     * @return array
     */
    private function replaceProductIdWithSku($products)
    {
        $productIds = array_column($products, 'product_id');

        if (empty($productIds)) {
            return $products;
        }

        $select = $this->resource->getConnection()->select()->from(
            $this->resource->getTableName('catalog_product_entity'),
            ['entity_id', 'sku']
        );
        $select->where('entity_id IN (?)', $productIds);
        $records = $this->resource->getConnection()->fetchPairs($select);

        foreach ($products as $key => $row) {
            // We may have mixed data, some with product id, some with sku.
            if (empty($row['product_id'])) {
                continue;
            }

            // For whatever reason if we are not able to find record
            // for this product id in DB, we don't do anything
            // for it.
            if (empty($records[$row['product_id']])) {
                unset($products[$key]);
                continue;
            }

            // Set sku in product data.
            $products[$key]['sku'] = $records[$row['product_id']];

            // Remove product id now.
            unset($products[$key]['product_id']);
        }

        return $products;
    }

}
