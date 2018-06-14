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

use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Catalog\Api\Data\ProductInterface;

use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\Data as ClientHelper;
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
     * Connector Product Update Endpoint
     * @const ENDPOINT_PRODUCT_UPDATE
     */
    const ENDPOINT_PRODUCT_UPDATE = 'ingest/product';

    /**
     * Magento WebAPI Service Class Name (for output formatting)
     * @var string $serviceClassName
     */
    protected $serviceClassName = 'Magento\Catalog\Api\ProductRepositoryInterface';

    /**
     * Magento WebAPI Service Method Name (for output formatting)
     * @var string $serviceMethodName
     */
    protected $serviceMethodName = 'get';

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
     * ProductSaveObserver constructor.
     * @param StoreManager $storeManager
     * @param ProductRepositoryInterface $productRepository
     * @param AcmHelper $acmHelper
     * @param ClientHelper $helper
     * @param ServiceOutputProcessor $outputProcessor
     * @param LoggerInterface $logger
     */
    public function __construct(
        StoreManager $storeManager,
        ProductRepositoryInterface $productRepository,
        AcmHelper $acmHelper,
        ClientHelper $helper,
        ServiceOutputProcessor $outputProcessor,
        LoggerInterface $logger
    ) {
        $this->storeManager = $storeManager;
        $this->productRepository = $productRepository;
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
     * Send updated category data to Acquia Commerce Manager.
     *
     * @param Observer $observer Incoming Observer
     *
     * @return void
     */
    public function execute(Observer $observer)
    {
        $product = $observer->getEvent()->getProduct();
        $this->logger->notice(
            sprintf('ProductSaveObserver: save product %d.', $product->getId()),
            ['sku' => $product->getSku(), 'store_id' => $product->getStoreId()]
        );

        // If the product data being saved is the base / default values,
        // send updated store specific products as well (that may inherit
        // base field value updates) for all of the stores that the
        // product is assigned.

        $stores = $product->getStoreIds();

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

            $this->logger->notice(
                sprintf('ProductSaveObserver: sending product for store %d.', $storeId),
                ['sku' => $product->getSku(), 'id' => $product->getId()]
            );

            $storeProduct = $this->productRepository->getById(
                $product->getId(),
                false,
                $storeId
            );

            if ($storeProduct) {
                $this->sendProductData($storeProduct, $storeId);
            }
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

                    $this->sendProductData($storeProduct, $storeId, true);
                }
            }
        }

    }

    /**
     * sendProductData
     *
     * Send product data to the Connector API endpoint.
     *
     * @param ProductInterface $product Product to send
     * @param mixed $storeId Store ID (string/int)
     * @param bool $forceDisabled Force product to be sent as disabled
     *
     * @return void
     */
    private function sendProductData(ProductInterface $product, $storeId, $forceDisabled = false)
    {
        $record = $this->acmHelper->getProductDataForAPI($product);

        if ($forceDisabled) {
            $record['status'] = \Magento\Catalog\Model\Product\Attribute\Source\Status::STATUS_DISABLED;
        }

        $doReq = function ($client, $opt) use ($record) {
            // Commerce Connector spec says always send an array.
            $opt['json'] = [$record];

            return $client->post(self::ENDPOINT_PRODUCT_UPDATE, $opt);
        };

        $this->tryRequest($doReq, 'ProductSaveObserver', $storeId);
    }
}
