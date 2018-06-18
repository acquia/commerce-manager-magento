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
use Magento\Catalog\Api\ProductRepositoryInterface;
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
     * Magento Product Repository
     *
     * @var ProductRepositoryInterface
     */
    protected $productRepository;

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
     * @param ProductRepositoryInterface $productRepository
     * @param BatchHelper $productBatchHelper
     * @param ClientHelper $clientHelper
     */
    public function __construct(
        AcmHelper $acmHelper,
        ServiceOutputProcessor $outputProc,
        LoggerInterface $logger,
        ProductRepositoryInterface $productRepository,
        BatchHelper $productBatchHelper,
        ClientHelper $clientHelper
    ) {
        $this->productRepository = $productRepository;
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
        if($this->productBatchHelper->getMessageQueueEnabled())
        {
            $batchSize = $this->productBatchHelper->getProductPushBatchSize();

            $batch = [];

            // Get bunch products.
            if ($products = $observer->getEvent()->getBunch()) {
                foreach ($products as $productRow) {
                    $sku = $productRow[ImportProduct::COL_SKU];

                    // Process only if there is SKU available in imported data.
                    if (empty($sku)) {
                        continue;
                    }

                    /** @var \Magento\Catalog\Api\Data\ProductInterface $product */
                    //NEEDS TRY CATCH
                    //Actually, you are running a product load then only storing the ID
                    //you could skip this, because the productPush tests for existence
                    //and instead store SKU in batch instead of ID (because here you really only have SKU)
                    $product = $this->productRepository->get($sku);

                    // Sanity check.
                    if (empty($product)) {
                        $this->logger->warning('ProductImportBunchSaveObserver: No product found, skipping.', [
                            'sku' => $sku,
                        ]);

                        continue;
                    }

                    $batch[] = $product->getId();

                    $this->logger->info('ProductImportBunchSaveObserver: Added product to queue for pushing.', [
                        'sku' => $sku,
                        'product_id' => $product->getId(),
                    ]);

                    // Push product ids in queue in batch.
                    // @TODO: Add website/store scope checks. See below mentioned
                    // class for example.
                    // Magento\CatalogUrlRewrite\Observer\AfterImportDataObserver
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
            }
        }
        else
        {
            $this->logger->warning('ProductImportBunchSaveObserver: Not EE. No message queue available. Imported products have not been batch-sent to Commerce Connector.');
        }
    }
}
