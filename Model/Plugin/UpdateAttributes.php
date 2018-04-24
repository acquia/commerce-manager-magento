<?php

/**
 * Acquia/CommerceManager/Model/Plugin/UpdateAttributes.php
 *
 * Acquia Commerce Manager UpdateAttributes Plugin
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Plugin;

use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\ProductBatch as BatchHelper;
use Magento\Catalog\Model\Product\Action\Interceptor;
use Psr\Log\LoggerInterface;

class UpdateAttributes
{

    /**
     * @var BatchHelper
     */
    private $batchHelper;

    /**
     * System Logger
     *
     * @var LoggerInterface $logger
     */
    protected $logger;

    /**
     * Constructor
     *
     * @param BatchHelper $batchHelper
     * @param LoggerInterface $logger
     */
    public function __construct(BatchHelper $batchHelper,
                                LoggerInterface $logger)
    {
        $this->batchHelper = $batchHelper;
        $this->logger = $logger;
    }

    /**
     * aroundUpdateAttributes
     *
     * @param callable $original
     * @param $productIds
     * @param $attrData
     * @param $storeId
     * @return Interceptor
     */
    public function aroundUpdateAttributes(
        Interceptor $interceptor,
        callable $original,
        $productIds,
        $attrData,
        $storeId
    )
    {
        // Execute the original method and remember the result.
        $result = $original($productIds, $attrData, $storeId);

        if ($this->batchHelper->pushOnProductAttributeUpdate()) {
            $productIds = array_unique($productIds);

            // Get batch size from config.
            $batchSize = $this->batchHelper->getProductPushBatchSize();

            // Do product push in batches.
            foreach (array_chunk($productIds, $batchSize, TRUE) as $chunk) {
                $batch = [];

                foreach ($chunk as $productId) {
                    $batch[$productId] = [
                        'product_id' => $productId,
                        'store_id' => $storeId,
                    ];
                }

                if (!empty($batch)) {
                    // Push product ids in queue in batch.
                    $this->batchHelper->addbatchtoqueue($batch);

                    $this->logger->info('Added products to queue for pushing in background.', [
                        'observer' => 'aroundUpdateAttributes',
                        'batch' => $batch,
                    ]);
                }
            }
        }

        return $result;
    }
}
