<?php

/**
 * Acquia/CommerceManager/Model/ProductSyncManagement.php
 *
 * Acquia Commerce Manager Product Syncronization Management
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Acquia\CommerceManager\Api\ProductSyncManagementInterface;
use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\ProductBatch as ProductBatchHelper;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Framework\Api\SearchCriteriaBuilder;
use Magento\Store\Model\StoreManagerInterface;
use Psr\Log\LoggerInterface;
use Magento\Catalog\Api\Data\ProductSearchResultsInterfaceFactory;

/**
 * ProductSyncManagement
 *
 * Acquia Commerce Manager Product Syncronization Management
 */
class ProductSyncManagement implements ProductSyncManagementInterface
{
    /**
     * @var AcmHelper $acmHelper
     */
    private $acmHelper;

    /**
     * Product Batch helper object.
     * @var ProductBatchHelper $batchHelper
     */
    private $batchHelper;

    /**
     * @var ProductRepositoryInterface $productRepository
     */
    private $productRepository;

    /**
     * @var SearchCriteriaBuilder $productRepository
     */
    private $searchCriteriaBuilder;

    /**
     * @var LoggerInterface $logger
     */
    protected $logger;

    /**
     * @var \Magento\Store\Model\StoreManagerInterface
     */
    protected $storeManager;

    /**
     * ProductSyncManagement constructor.
     *
     * @param AcmHelper $acmHelper
     * @param ProductBatchHelper $batchHelper
     * @param StoreManagerInterface $storeManager
     * @param SearchCriteriaBuilder $searchCriteriaBuilder
     * @param ProductRepositoryInterface $productRepository
     * @param LoggerInterface $logger
     */
    public function __construct(
        AcmHelper $acmHelper,
        ProductBatchHelper $batchHelper,
        StoreManagerInterface $storeManager,
        SearchCriteriaBuilder $searchCriteriaBuilder,
        ProductRepositoryInterface $productRepository,
        LoggerInterface $logger
    ) {
        $this->acmHelper = $acmHelper;
        $this->batchHelper = $batchHelper;
        $this->storeManager = $storeManager;
        $this->searchCriteriaBuilder = $searchCriteriaBuilder;
        $this->productRepository = $productRepository;
        $this->logger = $logger;
    }

    /**
     * {@inheritdoc}
     */
    public function syncProducts($page_count, $page_size = 50, $skus = '', $category_ids = '', $async = 1)
    {
        // IN conditions might probably not work as expected.
        // There is an issue already reported in Magento for this.
        // Check https://github.com/magento/magento2/issues/2892 for reference.
        // Set collection filters.
        if (!empty($skus))
        {
            $this->searchCriteriaBuilder->addFilter('sku', explode(',', $skus), 'in');
        }
        else
        {
            // Filter for active products.
            // We will skip this check if we are specifically asked to sync
            // some SKUs.
            $this->searchCriteriaBuilder->addFilter('status', Status::STATUS_ENABLED);
        }

        // Filter by category.
        if ($category_ids)
        {
            $this->searchCriteriaBuilder->addFilter('category_id', explode(',', $category_ids), 'in');
        }

        /** @var \Magento\Framework\Api\SearchCriteriaInterface $search_criteria */
        $search_criteria = $this->searchCriteriaBuilder->create();

        $search_criteria->setCurrentPage($page_count);
        $search_criteria->setPageSize($page_size);

        /** @var \Magento\Catalog\Api\Data\ProductInterface[] $products */
        $products = $this->productRepository->getList($search_criteria)->getItems();

        // Get current website id from context.
        $storeIdInContext = $this->storeManager->getStore()->getId();
        $websiteIdInContext = $this->storeManager->getStore($storeIdInContext)->getWebsiteId();

        // Format JSON Output
        $output = [];

        foreach ($products as $product) {
            $websiteIds = $product->getWebsiteIds();
            $record = $this->acmHelper->getProductDataForAPI($product);

            // If product is not in the website in context we will send
            // product with status disabled.
            if (!in_array($websiteIdInContext, $websiteIds)) {
                // Don't push this product if we are not specifically
                // asking for it.
                if (empty($skus)) {
                    $this->logger->debug('syncProducts: Product not available in website requested, not sending.', [
                        'sku' => $product->getSku(),
                        'id' => $product->getId(),
                        'store_id' => $storeIdInContext,
                        'website_id_in_context' => $websiteIdInContext,
                        'product_website_ids' => $websiteIds,
                    ]);

                    continue;
                }

                $this->logger->debug('syncProducts: Product not available in requested website, will send with status disabled.', [
                    'sku' => $product->getSku(),
                    'id' => $product->getId(),
                    'store_id' => $storeIdInContext,
                    'website_id_in_context' => $websiteIdInContext,
                    'product_website_ids' => $websiteIds,
                ]);

                $record['status'] = Status::STATUS_DISABLED;
            }

            $storeId = $record['store_id'];
            $output[$storeId][] = $record;
            $this->logger->info('Product sync maker. (store '.$storeId.')'.$product->getSku());
        }


        if ($async != 0) {
            $this->batchHelper->pushMultipleProducts($output, 'syncProducts');
            return (true);
        }
        else {
            return $output;
        }
    }
}
