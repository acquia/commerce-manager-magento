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
        $products = json_decode($batch, TRUE);

        if (empty($products) || !is_array($products)) {
            $this->logger->error("ProductPush: Invalid data received in consumer", [
                'batch' => $batch,
            ]);

            return;
        }

        $productDataByStore = [];

        $logData = [];
        $logData['pushed'] = [];
        $logData['start_time'] = microtime();

        foreach ($products as $row) {
            if (is_array($row)) {
                $productId = $row['product_id'];
                $storeIdRequested = $row['store_id'] ? $row['store_id'] : null;
            }
            // Backward compatibility. We need to update this everywhere
            // to push only for specific stores wherever possible.
            else {
                $productId = $row;
                $storeIdRequested = null;
            }

            // Force reload, we always want to send fresh data.
            $product = $this->productRepository->getById($productId, false, $storeIdRequested, true);

            // Do for only specific store if loaded product doesn't belong to default store.
            $stores = empty($storeIdRequested) ? $product->getStoreIds() : [$storeIdRequested];

            foreach ($stores as $storeId) {
                if ($storeId == 0) {
                    continue;
                }

                //NEEDS TRY CATCH
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

                if ($storeProduct) {
                    $logData['pushed'][] = [
                        'store_id' => $storeId,
                        'sku' => $storeProduct->getSku(),
                    ];

                    $productDataByStore[$storeId][] = $this->acmHelper->getProductDataForAPI($storeProduct);
                }
            }
        }

        $logData['before_api_call'] = microtime();

        $this->batchHelper->pushMultipleProducts($productDataByStore, 'pushProductBatch');

        $logData['end_time'] = microtime();
        $logData['pushed'] = json_encode($logData['pushed']);
        $this->logger->info('ProductPush: pushed products in background.', $logData);
    }
}
