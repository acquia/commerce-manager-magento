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

        foreach ($products as $row) {
            if (is_array($row)) {
                $productId = $row['product_id'];
                $storeId = $row['store_id'] ? $row['store_id'] : null;
            }
            // Backward compatibility. We need to update this everywhere
            // to push only for specific stores wherever possible.
            else {
                $productId = $row;
                $storeId = null;
            }

            // Force reload, we always want to send fresh data.
            $product = $this->productRepository->getById($productId, false, $storeId, true);

            // Do for only specific store if loaded product doesn't belong to default store.
            $stores = empty($storeId) ? $product->getStoreIds() : [$storeId];

            foreach ($stores as $storeId) {
                if ($storeId == 0) {
                    continue;
                }

                $this->logger->notice(
                    sprintf('ProductPush: sending product for store %d.', $storeId),
                    [ 'sku' => $product->getSku(), 'id' => $product->getId() ]
                );
//NEEDS TRY CATCH
                // Force reload, we always want to send fresh data.
                $storeProduct = $this->productRepository->getById(
                    $product->getId(),
                    false,
                    $storeId,
                    true
                );

                if ($storeProduct) {
                    $productDataByStore[$storeId][] = $this->acmHelper->getProductDataForAPI($storeProduct);
                }
            }
        }

        $this->batchHelper->pushMultipleProducts($productDataByStore, 'pushProductBatch');
    }
}
