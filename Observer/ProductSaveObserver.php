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
use Psr\Log\LoggerInterface;

/**
 * ProductSaveObserver
 *
 * Acquia Commerce Connector Product Save Observer
 */
class ProductSaveObserver extends ConnectorObserver implements ObserverInterface
{

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
     * ProductSaveObserver constructor.
     * @param StoreManager $storeManager
     * @param ProductRepositoryInterface $productRepository
     * @param AcmHelper $acmHelper
     * @param ProductBatchHelper $batchHelper
     * @param ClientHelper $helper
     * @param ServiceOutputProcessor $outputProcessor
     * @param LoggerInterface $logger
     */
    public function __construct(
        StoreManager $storeManager,
        ProductRepositoryInterface $productRepository,
        AcmHelper $acmHelper,
        ProductBatchHelper $batchHelper,
        ClientHelper $helper,
        ServiceOutputProcessor $outputProcessor,
        LoggerInterface $logger
    ) {
        $this->storeManager = $storeManager;
        $this->productRepository = $productRepository;
        $this->batchHelper = $batchHelper;
        parent::__construct(
            $acmHelper,
            $helper,
            $outputProcessor,
            $logger
        );
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
        $output = [];

        /** @var \Magento\Catalog\Model\Product $product */
        $product = $observer->getEvent()->getProduct();

        $need_push = FALSE;
        // If product status is changed.
        if (!empty($product->getOrigData())
          && $product->getData(ProductInterface::STATUS) != $product->getOrigData(ProductInterface::STATUS)) {
            $need_push = TRUE;
        }

        $this->logger->debug('ProductSaveObserver: saved product.', [
            'sku' => $product->getSku(),
            'id' => $product->getId(),
            'store_id' => $product->getStoreId(),
        ]);

        // If the product data being saved is the base / default values,
        // send updated store specific products as well (that may inherit
        // base field value updates) for all of the stores that the
        // product is assigned.
        $stores = $product->getStoreId() == 0 ? $product->getStoreIds() : [$product->getStoreId()];

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

            // Don't push if product status not changed and product is disabled.
            if (!$need_push && $storeProduct->getData(ProductInterface::STATUS) == Status::STATUS_DISABLED) {
                $this->logger->info('ProductSaveObserver: not pushing disabled product to ACM.', [
                  'sku' => $storeProduct->getSku(),
                  'store_id' => $storeId,
                ]);
                continue;
            }

            $this->logger->debug('ProductSaveObserver: sending product.', [
                'sku' => $storeProduct->getSku(),
                'id' => $storeProduct->getId(),
                'store_id' => $storeId,
            ]);

            $output[$storeId][] = $this->acmHelper->getProductDataForAPI($storeProduct);
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
            $this->logger->debug('ProductSaveObserver: product removed from websites.', [
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

                    $this->logger->debug('ProductSaveObserver: Product removed from website, will send product with status disabled.', [
                        'sku' => $storeProduct->getSku(),
                        'id' => $storeProduct->getId(),
                        'store_id' => $storeId,
                    ]);

                    $record = $this->acmHelper->getProductDataForAPI($storeProduct);
                    $record['status'] = \Magento\Catalog\Model\Product\Attribute\Source\Status::STATUS_DISABLED;
                    $output[$storeId][] = $record;
                }
            }
        }

        if ($output) {
            $this->batchHelper->pushMultipleProducts($output, 'productSave');
        }
    }

}
