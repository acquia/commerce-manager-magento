<?php

/**
 * Acquia/CommerceManager/Model/ProductPush
 *
 * Acquia Commerce Manager - Process items in queue to push products in background.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Acquia\CommerceManager\Helper\Acm;
use Acquia\CommerceManager\Helper\ProductBatch as BatchHelper;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Catalog\Model\Product\Attribute\Source\Status as ProductAttributeStatus;
use Psr\Log\LoggerInterface;

class ProductPush
{
    /**
     * @var ProductRepositoryInterface
     */
    private $productRepository;

    /**
     * @var BatchHelper
     */
    private $batchHelper;

    /**
     * @var Acm
     */
    private $acmHelper;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * ProductPush constructor.
     *
     * @param ProductRepositoryInterface $productRepository
     * @param BatchHelper $batchHelper
     * @param Acm $acmHelper
     * @param LoggerInterface $logger
     */
    public function __construct(
      ProductRepositoryInterface $productRepository,
      BatchHelper $batchHelper,
      Acm $acmHelper,
      LoggerInterface $logger
    ) {
        $this->productRepository = $productRepository;
        $this->batchHelper = $batchHelper;
        $this->acmHelper = $acmHelper;
        $this->logger = $logger;
    }

    /**
     * Push products in batch.
     *
     * @param string $batch
     */
    public function pushProductBatch($batch)
    {
        $products = json_decode($batch, true);

        if (empty($products) || !is_array($products)) {
            $this->logger->error("ProductPush: Invalid data received in consumer", [
                'batch' => $batch,
            ]);

            return;
        }

        foreach (array_chunk($products, $this->batchHelper->getProductPushBatchSize()) as $chunk) {
            $this->pushProductBatchChunk($chunk);
        }
    }

    /**
     * Push products in batch.
     *
     * @param array $products
     *   Products in single batch.
     */
    public function pushProductBatchChunk($products)
    {
        $productDataByStore = [];

        $logData = [];
        $logData['pushed'] = [];
        $logData['start_time'] = microtime();

        foreach ($products as $row) {
            try {
                if ($this->batchHelper->isProductPushReduceDuplicatesEnabled()) {
                    $this->batchHelper->setProductAsPushedFromQueue($row);
                }
                $productId = $sku = null;

                if (is_array($row) && isset($row['product_id'])) {
                    $productId = $row['product_id'];
                    $storeIdRequested = $row['store_id'] ? $row['store_id'] : null;
                }
                elseif (is_array($row) && isset($row['sku'])) {
                    $sku = $row['sku'];
                    $storeIdRequested = $row['store_id'] ? $row['store_id'] : null;
                }
                // Backward compatibility. We need to update this everywhere
                // to push only for specific stores wherever possible.
                else {
                    $productId = $row;
                    $storeIdRequested = null;
                }

                // Force reload, we always want to send fresh data.
                /** @var \Magento\Catalog\Api\Data\ProductInterface $product */
                $product = isset($productId)
                    ? $this->productRepository->getById($productId, false, $storeIdRequested, true)
                    : $this->productRepository->get($sku, false, $storeIdRequested, true);

                // Sanity check.
                if (empty($product)) {
                    continue;
                }

                $productStoreIds = $product->getStoreIds();

                // Do for only specific store if loaded product doesn't belong to default store.
                $stores = empty($storeIdRequested) ? $productStoreIds : [$storeIdRequested];

                foreach ($stores as $storeId) {
                    if ($storeId == 0) {
                        continue;
                    }

                    // Force reload, we always want to send fresh data.
                    // If product push was requested only for one store, we have
                    // loaded that already outside loop. Let's use it as is.
                    $storeProduct = ($storeId === $storeIdRequested)
                        ? $product
                        : $this->productRepository->getById(
                            $product->getId(),
                            false,
                            $storeId,
                            true
                        );

                    if (empty($storeProduct)) {
                        continue;
                    }

                    $logData['pushed'][] = [
                        'store_id' => $storeId,
                        'sku' => $storeProduct->getSku(),
                    ];

                    $record = $this->acmHelper->getProductDataForAPI($storeProduct);

                    // For stores not currently assigned to product, we send to Drupal
                    // as disabled.
                    if (!in_array($storeId, $productStoreIds)) {
                        $record['status'] = ProductAttributeStatus::STATUS_DISABLED;
                    }

                    $productDataByStore[$storeId][] = $record;
                }
            }
            catch (\Exception $e) {
                $this->logger->warning('Failed to push product from queue.', [
                    'row' => json_encode($row),
                    'exception' => $e->getMessage(),
                ]);
            }
            catch (\Throwable $e) {
                $this->logger->warning('Failed to push product from queue.', [
                    'row' => json_encode($row),
                    'exception' => $e->getMessage(),
                ]);
            }
        }

        $logData['before_api_call'] = microtime();

        $this->batchHelper->pushMultipleProducts($productDataByStore, 'pushProductBatch');

        $logData['end_time'] = microtime();
        $logData['pushed'] = json_encode($logData['pushed']);
        $this->logger->info('ProductPush: pushed products in background.', $logData);
    }

}
