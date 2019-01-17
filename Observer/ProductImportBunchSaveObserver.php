<?php

/**
 * Acquia/CommerceManager/Observer/ProductImportBunchSaveObserver.php
 *
 * Acquia Commerce Connector ProductImportBunch Save Observer
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Observer;

use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\ProductBatch as BatchHelper;
use Magento\Framework\Event\ObserverInterface;
use Magento\Framework\Event\Observer;
use Magento\Framework\Webapi\ServiceOutputProcessor;
use Magento\CatalogImportExport\Model\Import\Product as ImportProduct;
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
     * ProductImportBunchSaveObserver constructor.
     *
     * @param AcmHelper $acmHelper
     * @param ServiceOutputProcessor $outputProc
     * @param LoggerInterface $logger
     * @param BatchHelper $productBatchHelper
     * @param ClientHelper $clientHelper
     */
    public function __construct(
        AcmHelper $acmHelper,
        ServiceOutputProcessor $outputProc,
        LoggerInterface $logger,
        BatchHelper $productBatchHelper,
        ClientHelper $clientHelper
    ) {
        $this->productBatchHelper = $productBatchHelper;
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

        $batchSize = (int) $this->acmHelper->getProductQueueBatchSize();

        $batch = [];

        // Get bunch products.
        if ($products = $observer->getEvent()->getBunch()) {
            $logData = [];

            foreach ($products as $productRow) {
                $sku = $productRow[ImportProduct::COL_SKU];

                // Process only if there is SKU available in imported data.
                if (empty($sku)) {
                    continue;
                }

                // @TODO: Implement checks for store/website values and
                // use them if available. It is possible to import without
                // store/website code. If available, it can reduce number
                // of Product Pushes. See below mentioned class for example.
                // \Magento\CatalogUrlRewrite\Observer\AfterImportDataObserver
                // $store_code = $productRow[ImportProduct::COL_STORE_VIEW_CODE];
                // $website_code = $productRow[ImportProduct::COL_WEBSITE];
                $batch[$sku] = [
                    'sku' => $sku,
                    'store_id' => NULL,
                ];

                $logData[$sku] = $sku;

                // Push product ids in queue in batch.
                // Playing safe with >= instead of ==.
                if (count($batch) >= $batchSize) {
                    $this->productBatchHelper->addBatchToQueue($batch);

                    // Reset batch.
                    $batch = [];
                }
            }

            // Push product ids in last batch (which might be lesser in count
            // than batch size.
            if (!empty($batch)) {
                $this->productBatchHelper->addBatchToQueue($batch);
            }

            $this->logger->info('ProductImportBunchSaveObserver: Added products to queue for pushing.', [
                'skus' => implode(',', $logData),
            ]);
        }
    }
}
