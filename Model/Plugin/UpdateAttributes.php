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
     * Constructor
     *
     * @param BatchHelper $batchHelper
     * @param StoreManager $storeManager
     * @param Repository $productAttributeRepository
     */
    public function __construct(BatchHelper $batchHelper,
                                StoreManager $storeManager,
                                Repository $productAttributeRepository)
    {
        $this->batchHelper = $batchHelper;
        $this->storeManager = $storeManager;
        $this->productAttributeRepository = $productAttributeRepository;
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
            return $result;
        }
        // Push to all stores by default.
        $storeIds = [0];

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

        $productsToQueue = [];
        foreach ($productIds as $productId) {
            $productsToQueue[] = [
                'product_id' => $productId,
                'stores' => $storeIds,
            ];
        }

        // Queue products to be pushed in background.
        if (!empty($productsToQueue)) {
            $this->acmHelper->addProductsToQueue($productsToQueue, __CLASS__ . ':' . __FUNCTION__);
        }

        return $result;
    }

}