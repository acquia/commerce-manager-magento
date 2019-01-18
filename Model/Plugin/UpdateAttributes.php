<?php

/**
 * Acquia/CommerceManager/Model/Plugin/UpdateAttributes.php
 *
 * Acquia Commerce Manager UpdateAttributes Plugin
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Plugin;

use Acquia\CommerceManager\Helper\ProductBatch as BatchHelper;
use Acquia\CommerceManager\Model\Product\Attribute\Repository;
use Magento\Catalog\Model\Product\Action;
use Magento\Store\Model\StoreManager;
use Psr\Log\LoggerInterface;

class UpdateAttributes
{

    /**
     * @var BatchHelper
     */
    private $batchHelper;

    /**
     * Store manager.
     *
     * @var StoreManager $storeManager
     */
    protected $storeManager;

    /**
     * Product attribute repository
     *
     * @var Repository $productAttributeRepository
     */
    protected $productAttributeRepository;

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
     * @param StoreManager $storeManager
     * @param Repository $productAttributeRepository
     * @param LoggerInterface $logger
     */
    public function __construct(BatchHelper $batchHelper,
                                StoreManager $storeManager,
                                Repository $productAttributeRepository,
                                LoggerInterface $logger)
    {
        $this->batchHelper = $batchHelper;
        $this->storeManager = $storeManager;
        $this->productAttributeRepository = $productAttributeRepository;
        $this->logger = $logger;
    }

    /**
     * aroundUpdateAttributes
     *
     * @param Action $subject
     * @param callable $original
     * @param $productIds
     * @param $attrData
     * @param $storeId
     * @return Action
     */
    public function aroundUpdateAttributes(
        Action $subject,
        callable $original,
        $productIds,
        $attrData,
        $storeId
    )
    {
        // Execute the original method and remember the result.
        $result = $original($productIds, $attrData, $storeId);

        if ($this->batchHelper->pushOnProductAttributeUpdate()) {
            // Push to all stores by default.
            $storeIds = [NULL];

            // If not the default store view.
            if ($storeId != 0) {
                // Push to current store if set.
                $storeIds = [$storeId];

                // Check for attributes being updated.
                foreach (array_keys($attrData) as $attr_code) {
                    $attribute = $this->productAttributeRepository->get($attr_code);

                    // If any attribute is at website scope level then we need to
                    // push for all stores of the website.
                    if ($attribute->isScopeWebsite()) {
                        // Get all stores belong same website as current store.
                        $storeIds = $this->storeManager->getStore($storeId)->getWebsite()->getStoreIds();
                        $this->logger->debug('Updated attribute is website level so pushing only for all stores of website.', [
                            'attribute_code' => $attr_code,
                            'store_ids' => implode(',', $storeIds),
                        ]);
                        break;
                    }
                }
            }

            $productIds = array_unique($productIds);

            // Get batch size from config.
            $batchSize = $this->batchHelper->getProductQueueBatchSize();

            // Do product push in batches.
            foreach (array_chunk($productIds, $batchSize, TRUE) as $chunk) {
                $batch = [];

                foreach ($chunk as $productId) {
                    // Send to multiple stores.
                    foreach ($storeIds as $store_id) {
                        $batch[$store_id][] = [
                            'product_id' => $productId,
                            'store_id' => $store_id,
                        ];
                    }
                }

                if (!empty($batch)) {
                    // Push product ids in queue in batch.
                    foreach ($batch as $storeBatch) {
                        $this->batchHelper->addbatchtoqueue($storeBatch);

                        $this->logger->info('Added products to queue for pushing in background.', [
                            'observer' => 'aroundUpdateAttributes',
                            'batch' => $storeBatch,
                        ]);
                    }
                }
            }
        }

        return $result;
    }
}
