<?php

/**
 * Acquia/CommerceManager/Observer/ProductSaveObserver.php
 *
 * Acquia Commerce Connector Product Save Observer
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Observer;

use Magento\Framework\Event\ObserverInterface;
use Magento\Framework\Event\Observer;

use Magento\Catalog\Api\Data\ProductInterface;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Catalog\Model\Product\Attribute\Source\Status;

use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Acquia\CommerceManager\Helper\ProductBatch as ProductBatchHelper;
use Magento\Framework\Webapi\ServiceOutputProcessor;
use Magento\Store\Model\StoreManager;
use Magento\Framework\Message\ManagerInterface as MessageManager;
use Psr\Log\LoggerInterface;

/**
 * ProductSaveObserver
 *
 * Acquia Commerce Connector Product Save Observer
 */
class ProductSaveObserver extends ConnectorObserver implements ObserverInterface
{

    /**
     * SKU data that needs to be pushed to the queue later in the process, once
     * product save transaction is committed.
     *
     * @var array
     *   SKU data that to be push to queue.
     */
    public static $dataToPush = [];

    /**
     * Magento Product Repository
     * @var ProductRepositoryInterface $productRepository
     */
    private $productRepository;

    /**
     * Magento Store Manager
     * @var StoreManager $storeManager
     */
    private $storeManager;

    /**
     * Product Batch helper object.
     * @var ProductBatchHelper $batchHelper
     */
    private $batchHelper;

    /**
     * Message manager
     * @var MessageManager $messageManager
     */
    private $messageManager;

    /**
     * ProductSaveObserver constructor.
     * @param StoreManager $storeManager
     * @param ProductRepositoryInterface $productRepository
     * @param AcmHelper $acmHelper
     * @param ProductBatchHelper $batchHelper
     * @param ClientHelper $helper
     * @param ServiceOutputProcessor $outputProcessor
     * @param LoggerInterface $logger
     * @param MessageManager $messageManager
     */
    public function __construct(
        StoreManager $storeManager,
        ProductRepositoryInterface $productRepository,
        AcmHelper $acmHelper,
        ProductBatchHelper $batchHelper,
        ClientHelper $helper,
        ServiceOutputProcessor $outputProcessor,
        LoggerInterface $logger,
        MessageManager $messageManager
    ) {
        $this->storeManager = $storeManager;
        $this->productRepository = $productRepository;
        $this->batchHelper = $batchHelper;
        $this->messageManager = $messageManager;
        parent::__construct(
            $acmHelper,
            $helper,
            $outputProcessor,
            $logger
        );

        register_shutdown_function([$this, 'pushToQueue']);
    }

    /**
     * execute
     *
     * Send updated product data to Acquia Commerce Manager.
     *
     * @param Observer $observer Incoming Observer
     *
     * @return void
     */
    public function execute(Observer $observer)
    {
        $storeIds = [];

        /** @var \Magento\Catalog\Model\Product $product */
        $product = $observer->getEvent()->getProduct();

        $this->logger->info('ProductSaveObserver: saved product.', [
            'sku' => $product->getSku(),
            'id' => $product->getId(),
            'store_id' => $product->getStoreId(),
        ]);

        // If the product data being saved is the base / default values,
        // send updated store specific products as well (that may inherit
        // base field value updates) for all of the stores that the
        // product is assigned.
        $stores = $product->getStoreId() == 0 ? $product->getStoreIds() : [$product->getStoreId()];

        // In case of specific store update there may be larger scope
        // fields updated.
        if ($product->getStoreId() != 0 && !empty($product->getOrigData())) {
            $attr_changed = null;
            // Push to all stores on category change.
            if (
                (empty($product->getOrigData('category_ids')) && !empty($product->getData('category_ids')))
                || (!empty($product->getOrigData('category_ids')) && empty($product->getData('category_ids')))
                || (
                    !empty($product->getOrigData('category_ids'))
                    && !empty($product->getData('category_ids'))
                    &&
                    (!empty(array_diff($product->getData('category_ids'), $product->getOrigData('category_ids')))
                        || !empty(array_diff($product->getOrigData('category_ids'), $product->getData('category_ids')))
                    ))
            ) {
                $stores = $product->getStoreIds();
                $attr_changed = 'category';
            }
            // Push to all stores of website if we are enabling the product
            // so all translations are created.
            elseif ($product->getData('status') != $product->getOrigData('status')
                && $product->getData('status') == Status::STATUS_ENABLED) {
                // Push to all stores of the website on status change.
                $stores = $this->storeManager->getStore($product->getStoreId())->getWebsite()->getStoreIds();
                $attr_changed = 'status';
            }

            // If any change, only then log.
            if ($attr_changed) {
                $this->logger->info('ProductSaveObserver: pushing products to more stores as there is change in attribute.', [
                    'sku' => $product->getSku(),
                    'stores' => implode(',', $stores),
                    'attr_code' => $attr_changed,
                ]);
            }
        }

        foreach ($stores as $storeId) {
            // Never send the admin store.
            if ($storeId == 0) {
                continue;
            }

            // Only send data for active stores
            /** @var \Magento\Store\Model\Store $storeModel */
            $storeModel = $this->storeManager->getStore($storeId);
            if (!$storeModel->isActive()) {
                continue;
            }

            $storeProduct = $this->productRepository->getById(
                $product->getId(),
                false,
                $storeId
            );

            if (empty($storeProduct)) {
                continue;
            }

            // Avoid pushing disabled products when not needed.
            $do_not_push_disabled = false;
            if (empty($product->getOrigData())) {
                // Case of a creation.
                $do_not_push_disabled = $storeProduct->getData(ProductInterface::STATUS) == Status::STATUS_DISABLED;
            }
            else {
                // Case of an update on default store or when update on specific store
                // with pushing store not matching with current product store.
                if ($product->getStoreId() == 0 || $storeId != $product->getStoreId()) {
                    // Case of an update on default store view.
                    $do_not_push_disabled = $storeProduct->getData(ProductInterface::STATUS) == Status::STATUS_DISABLED && !($product->getOrigData(ProductInterface::STATUS) == Status::STATUS_ENABLED && $product->getData(ProductInterface::STATUS) == Status::STATUS_DISABLED);
                }
                else {
                    // Case of an update on specific store.
                    $do_not_push_disabled = $product->getOrigData(ProductInterface::STATUS) == Status::STATUS_DISABLED && $product->getData(ProductInterface::STATUS) == Status::STATUS_DISABLED;
                }
            }

            if ($do_not_push_disabled) {
                $this->logger->info('ProductSaveObserver: not pushing disabled product to ACM.', [
                    'sku' => $storeProduct->getSku(),
                    'store_id' => $storeId,
                ]);
                continue;
            }

            $storeIds[] = $storeId;
        }

        // For the sites in which product is removed, we will send the product
        // with status disabled to ensure remote system gets an update.
        $websiteIdsOriginal = $product->getOrigData('website_ids');
        $websiteIds = $product->getWebsiteIds();

        // If an event is triggered manually, we won't get $websiteIdsOriginal.
        $websitesIdsRemoved = is_array($websiteIdsOriginal)
            ? array_diff($websiteIdsOriginal, $websiteIds)
            : [];

        if ($websitesIdsRemoved) {
            $this->logger->info('ProductSaveObserver: product removed from websites.', [
                'sku' => $product->getSku(),
                'id' => $product->getId(),
                'website_ids_removed' => $websitesIdsRemoved,
            ]);

            foreach ($websitesIdsRemoved as $websiteId) {
                $website = $this->storeManager->getWebsite($websiteId);
                foreach ($website->getStoreIds() as $storeId) {
                    $storeProduct = $this->productRepository->getById(
                        $product->getId(),
                        false,
                        $storeId
                    );

                    // Ideally we should not get product here but for some reason Magento
                    // gives full loaded product with status same as default store, which
                    // would be enabled most of the time.
                    if (empty($storeProduct)) {
                        continue;
                    }

                    $this->logger->info('ProductSaveObserver: Product removed from website, queuing product to be pushed.', [
                        'sku' => $storeProduct->getSku(),
                        'id' => $storeProduct->getId(),
                        'store_id' => $storeId,
                    ]);

                    $storeIds[] = $storeId;
                }
            }
        }

        if (empty($storeIds)) {
            return;
        }

        self::$dataToPush[] = [
            'sku' => $product->getSku(),
            'stores' => $storeIds,
        ];

        if ($this->batchHelper->getMessageQueueEnabled()) {
            $this->messageManager->addNotice(__('Your product update has been pushed to ProductPush queue of Magento. Once processed it is going to be pushed to ACM for every impacted stores and queued there.'));
        }
        else {
            $this->messageManager->addNotice(__('Your product assignments have been pushed to ACM for every impacted stores and are going to be queued there.'));
        }
    }

    /**
     * ShutDown function.
     *
     * This shutdown function is used to push product data to the queue later in
     * the PHP process to avoid pushing stale/obsolete data. This is happening
     * because the product save is done in a database transaction that is not yet
     * committed at the time of the observer.
     */
    public function pushToQueue()
    {
        if (empty(self::$dataToPush) || !is_array(self::$dataToPush)) {
            return;
        }

        foreach (self::$dataToPush as $data) {
            if ($this->batchHelper->getMessageQueueEnabled()) {
                $this->batchHelper->addProductsToQueue([$data], __METHOD__, false);
                $this->logger->info('Data pushed to queue.', [
                    'sku' => $data['sku'],
                    'stores' => implode(',', $data['stores']),
                ]);
            }
            else {
                foreach ($data['stores'] as $storeId) {
                    $product = $this->productRepository->get($data['sku'], false, $storeId);
                    $record = $this->acmHelper->getProductDataForAPI($product);
                    $productDataByStore[$storeId][] = $record;
                }

                $this->batchHelper->pushMultipleProducts($productDataByStore, 'ProductSaveObserver');
            }
        }
    }

}
