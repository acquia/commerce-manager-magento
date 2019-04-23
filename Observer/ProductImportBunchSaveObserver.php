<?php

/**
 * Acquia/CommerceManager/Observer/ProductImportBunchSaveObserver.php
 *
 * Acquia Commerce Connector ProductImportBunch Save Observer
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Observer;

use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\ProductBatch as BatchHelper;
use Magento\Framework\Event\ObserverInterface;
use Magento\Framework\Event\Observer;
use Magento\Framework\Webapi\ServiceOutputProcessor;
use Magento\CatalogImportExport\Model\Import\Product as ImportProduct;
use Magento\Framework\Message\ManagerInterface as MessageManager;
use Psr\Log\LoggerInterface;

/**
 * ProductImportBunchSaveObserver
 *
 * Acquia Commerce Connector ProductImportBunch Save Observer
 */
class ProductImportBunchSaveObserver extends ConnectorObserver implements ObserverInterface
{

    /**
     * BatchHelper class object.
     *
     * @var BatchHelper
     */
    protected $productBatchHelper;

    /**
     * Message manager.
     *
     * @var MessageManager
     */
    protected $messageManager;

    /**
     * ProductImportBunchSaveObserver constructor.
     *
     * @param AcmHelper $acmHelper
     * @param ServiceOutputProcessor $outputProc
     * @param LoggerInterface $logger
     * @param BatchHelper $productBatchHelper
     * @param ClientHelper $clientHelper
     * @param MessageManager $messageManager
     */
    public function __construct(
        AcmHelper $acmHelper,
        ServiceOutputProcessor $outputProc,
        LoggerInterface $logger,
        BatchHelper $productBatchHelper,
        ClientHelper $clientHelper,
        MessageManager $messageManager
    ) {
        $this->productBatchHelper = $productBatchHelper;
        $this->messageManager = $messageManager;
        parent::__construct(
            $acmHelper,
            $clientHelper,
            $outputProc,
            $logger);
    }

    /**
     * execute
     *
     * Send imported product data to Acquia Commerce Manager.
     *
     * @param Observer $observer Incoming Observer
     *
     * @return void
     */
    public function execute(Observer $observer)
    {
        // EE only. Later please remove this conditional
        // after you have coded a CE equivalent using Magento CRON
        if(!$this->productBatchHelper->getMessageQueueEnabled()) {
            $this->logger->warning('ProductImportBunchSaveObserver: Not EE. No message queue available. Imported products have not been batch-sent to Commerce Connector.');
            return;
        }

        $products = $observer->getEvent()->getBunch();
        if (empty($products) || empty($products[0])) {
            $this->logger->warning('ProductImportBunchSaveObserver: Invoked with empty products data in observer event.');
            return;
        }

        if (!isset($products[0][ImportProduct::COL_SKU])) {
            $this->logger->warning('ProductImportBunchSaveObserver: Invoked with products data without sku in data, ignoring.');
            return;
        }

        $skus = array_column($products, ImportProduct::COL_SKU);
        $store_ids = $this->acmHelper->getAllActiveStoreIds();
        $statuses = $this->acmHelper->getProductStatusForStores($skus, $store_ids);

        // When status column is set, we push to all stores of particular website if store/website set.
        // If store/website is not set, we will simply push to all the stores (even if disabled).
        $status_column_set = isset($products[0]['status']);

        $batch = [];
        $logData = [];
        foreach ($products as $productRow) {
            $sku = $productRow[ImportProduct::COL_SKU];
            $productStoresToPush = [];

            // If store code is set in import, we will import only for
            // that specific store.
            if (isset($productRow[ImportProduct::COL_STORE_VIEW_CODE])) {
                $store_code = $productRow[ImportProduct::COL_STORE_VIEW_CODE];

                // Push to all stores of a site when updating status.
                if ($status_column_set) {
                    $productStoresToPush = $this->acmHelper->getAllStoresInWebsiteForStore($store_code);
                }
                else {
                    $productStoresToPush = [$store_ids[$store_code]];
                }
            }
            // If website_code column is set in import, we will get only
            // one website id here, we will import for only the stores
            // available in that website.
            elseif (isset($productRow[ImportProduct::COL_WEBSITE])) {
                $productStoresToPush = $this->acmHelper->getAllStoresForWebsite($productRow[ImportProduct::COL_WEBSITE]);
            }
            // If _product_websites column is set, we need to get stores
            // for all the websites as it is multiple value field.
            elseif (isset($productRow[ImportProduct::COL_PRODUCT_WEBSITES])) {
                $separator = $observer->getData('adapter')->getMultipleValueSeparator();
                $websiteCodes = explode($separator, $productRow[ImportProduct::COL_PRODUCT_WEBSITES]);
                foreach ($websiteCodes as $websiteCode) {
                    $productStoresToPush = array_merge($productStoresToPush, $this->acmHelper->getAllStoresForWebsite($websiteCode));
                }
            }
            // Push to all the stores if we are not able to filter.
            else {
                $productStoresToPush = [0];
            }

            // Remove disabled stores if status column not set.
            if (!$status_column_set) {
                foreach ($productStoresToPush as $index => $storeId) {
                    if ($statuses[$sku][$storeId] == Status::STATUS_DISABLED) {
                        unset($productStoresToPush[$index]);
                    }
                }
            }

            // Skip the products completely that are not enabled in any stores.
            if (empty($productStoresToPush)) {
                continue;
            }

            foreach ($productStoresToPush as $storeId) {
                $batch[$storeId][$sku] = [
                    'sku' => $sku,
                    'store_id' => $storeId,
                ];
            }

            $logData[$sku] = $sku . ' (Store ids: ' . implode(',', $productStoresToPush) . ')';
        }

        // Simply return if nothing to queue.
        if (empty($batch)) {
            return;
        }

        $batchSize = $this->productBatchHelper->getProductQueueBatchSize();

        // Add batch to queue, create chunks per store id based on batch size.
        foreach ($batch as $storeBatch) {
            foreach (array_chunk($storeBatch, $batchSize, true) as $chunk) {
                $this->productBatchHelper->addBatchToQueue($chunk);
            }
        }

        $this->logger->info('ProductImportBunchSaveObserver: Added products to queue for pushing.', [
            'skus' => implode(', ', $logData),
        ]);

        if (!$this->productBatchHelper->getMessageQueueEnabled()) {
            $this->messageManager->addNotice(__('Your product assignments have been pushed to ACM for every impacted stores and are going to be queued there.'));
        }
        else {
            $this->messageManager->addNotice(__('Your product assignments have been pushed to ProductPush queue of Magento. Once processed they are going to be pushed to ACM for every impacted stores and queued there.'));
        }
    }
}
