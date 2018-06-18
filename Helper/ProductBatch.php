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
use Magento\Catalog\Api\Data\ProductInterface;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Framework\App\Helper\Context;
use Magento\Framework\Module\ModuleListInterface;
use Psr\Log\LoggerInterface;

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
     * ProductBatch constructor.
     *
     * @param Context $context
     * @param ProductRepositoryInterface $productRepository
     * @param Data $clientHelper
     * @param AcmHelper $acmHelper
     * @param ModuleListInterface $moduleList
     */
    public function __construct(
        Context $context,
        ProductRepositoryInterface $productRepository,
        ClientHelper $clientHelper,
        AcmHelper $acmHelper,
        ModuleListInterface $moduleList
    ) {
        $this->productRepository = $productRepository;
        $this->clientHelper = $clientHelper;
        $this->acmHelper = $acmHelper;
        $this->moduleList = $moduleList;
        $this->logger = $context->getLogger();
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
                $this->publisher = \Magento\Framework\App\ObjectManager::getInstance()->get(
                    \Magento\Framework\MessageQueue\PublisherInterface::class
                )->create();
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
     * Add batch (consisting of product ids) to queue.
     *
     * @param mixed $batch
     */
    public function addBatchToQueue($batch)
    {
        $batch = is_array($batch) ? $batch : [$batch];

        // MessageQueue is EE only. So do nothing if there is no queue.
        $messageQueue = $this->getMessageQueue();
        if($messageQueue) {
            $this->getMessageQueue()->publish(self::PRODUCT_PUSH_CONSUMER, json_encode($batch));
        } else {
            // At 20180123, do nothing.
            // Later (Malachy or Anuj): Use Magento CRON instead
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

}
