<?php

/**
 * Acquia/CommerceManager/Model/Indexer/SalesRuleBuilder.php
 *
 * Acquia Commerce Manager Sales Rule / Product Index Builder
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Indexer;

use Magento\CatalogUrlRewrite\Model\ResourceModel\Category\ProductCollection;
use Magento\Framework\App\ResourceConnection;
use Magento\Catalog\Model\Product\Attribute\Source\Status;
use Magento\Catalog\Model\ResourceModel\Product\CollectionFactory as ProductCollectionFactory;
use Magento\Rule\Model\Condition\Combine as CombineCondition;
use Magento\SalesRule\Model\Rule as SalesRule;
use Magento\SalesRule\Model\Rule\Condition\Product as ProductCondition;
use Magento\SalesRule\Model\ResourceModel\Rule\Collection as RuleCollection;
use Magento\SalesRule\Model\ResourceModel\Rule\CollectionFactory as RuleCollectionFactory;
use Magento\Store\Model\StoreManagerInterface;
use Psr\Log\LoggerInterface;

/**
 * SalesRuleBuilder
 *
 * Acquia Commerce Manager Sales Rule / Product Index Builder
 */
class SalesRuleBuilder
{
    /**
     * Database Transaction Batch Size
     * @var int $batchSize
     */
    protected $batchSize;

    /**
     * Magento Framework DB connection
     * @var ResourceConnection $connection
     */
    protected $connection;

    /**
     * Magento Product Collection Factory
     * @var ProductCollectionFactory $productCollection
     */
    protected $productCollection;

    /**
     * Magento DB Resource
     * @var ResourceConnection $resource
     */
    protected $resource;

    /**
     * Sales Rule Collection Factory
     * @var RuleCollectionFactory $ruleCollection
     */
    protected $ruleCollection;

    /**
     * Magento System Logger
     * @var LoggerInterface $logger
     */
    protected $logger;

    /**
     * Array to keep a check on processed products and avoid duplicates entries.
     *
     * @var array
     */
    protected $duplicatesCheck = [];

    /**
     * SalesRuleBuilder constructor.
     *
     * @param ProductCollectionFactory $productCollection
     * @param RuleCollectionFactory    $ruleCollection
     * @param ResourceConnection       $resource
     * @param StoreManagerInterface    $storeManager
     * @param LoggerInterface $logger
     * @param int                      $batchSize
     */
    public function __construct(
        ProductCollectionFactory $productCollection,
        RuleCollectionFactory $ruleCollection,
        ResourceConnection $resource,
        StoreManagerInterface $storeManager,
        LoggerInterface $logger,
        $batchSize = 1000
    ) {
        $this->productCollection = $productCollection;
        $this->ruleCollection = $ruleCollection;
        $this->resource = $resource;
        $this->connection = $resource->getConnection();
        $this->storeManager = $storeManager;
        $this->logger = $logger;
        $this->batchSize = $batchSize;
    }

    /**
     * reindexByProductIds
     *
     * Rebuild sales rule / product indexes by product IDs.
     *
     * @param int[] $ids Product Ids
     *
     * @return void
     */
    public function reindexByProductIds(array $ids)
    {
        $this->logger->info(
            'Reindexing by product IDS.',
            $ids
        );

        // Delete existing product rows
        $this->cleanByProductIds($ids);

        // Generate new product indexes

        $buildProducts = function () use ($ids) {
            return ($this->getProductCollection()->addIdFilter($ids));
        };

        $rules = $this->getRuleCollection();

        $this->buildProductRuleIndex($rules, $buildProducts);
    }

    /**
     * reindexByRuleIds
     *
     * Rebuild sales rule / product indexes by rule IDS.
     *
     * @param int[] $ids Sales Rule Ids
     *
     * @return void
     */
    public function reindexByRuleIds(array $ids)
    {
        $this->logger->info(
            'Reindexing by sales rule IDS.',
            $ids
        );

        // Delete existing rule rows
        $this->cleanByRuleIds($ids);

        // Generate new rule indexes
        $rules = $this->getRuleCollection()
            ->addFieldToFilter('rule_id', ['in' => $ids]);

        $this->buildProductRuleIndex($rules);
    }

    /**
     * reindexFull
     *
     * Rebuild the full sales rule / product index.
     *
     * @return void
     */
    public function reindexFull()
    {
        $this->logger->info('reindexFull: reindexing.');

        // Truncate existing index
        $this->cleanAll();

        // Build new product indexes for each rule
        $rules = $this->getRuleCollection();

        $this->buildProductRuleIndex($rules);
    }

    /**
     * buildProductRuleIndex
     *
     * Iterate rules provided for indexing and generate matching product
     * collections for each rule, then apply the rule to each product
     * and index discount results.
     *
     * @param RuleCollection $rules Sales Rules Collection
     * @param Callable $buildProducts Product collection building closure
     *
     * @return void
     */
    protected function buildProductRuleIndex(
        RuleCollection $rules,
        $buildProducts = null
    ) {
        if (!$buildProducts || !is_callable($buildProducts)) {
            $buildProducts = function () {
                return ($this->getProductCollection());
            };
        }

        /** @var SalesRule $rule */
        foreach ($rules as $rule) {
            // We index only for those without coupons.
            if ($rule->getCouponType() != SalesRule::COUPON_TYPE_NO_COUPON) {
                continue;
            }

            $products_processed = FALSE;

            $conditions = $this->locateProductConditions(
                $rule->getConditions()->getConditions()
            );

            try {
                if (!empty($conditions)) {
                    // Assemble matching products collection to rule conditions
                    $products = $buildProducts();
                    $this->filterProducts($products, $conditions);
                    $this->addProductsToIndex($products, $rule);
                    $products_processed = TRUE;
                }
            }
            catch (\RuntimeException $e) {
                continue;
            }

            $actionConditions = $this->locateProductConditions(
                $rule->getActions()->getConditions()
            );

            try {
                if (!empty($actionConditions)) {
                    // Assemble matching products collection to rule conditions
                    $actionProducts = $buildProducts();
                    $this->filterProducts($actionProducts, $actionConditions);
                    $this->addProductsToIndex($actionProducts, $rule);
                    $products_processed = TRUE;
                }
            }
            catch (\RuntimeException $e) {
                continue;
            }

            if (!$products_processed) {
                $this->logger->warning('No products conditions available for rule. Labels wont be displayed', [
                    $rule->getId(),
                ]);
            }
        }
    }

    /**
     * addProductsToIndex.
     *
     * Helper function to add filtered products to index.
     *
     * @param $products
     * @param SalesRule $rule
     */
    protected function addProductsToIndex($products, SalesRule $rule) {
        $rows = [];

        $ruleWebsiteIds = $rule->getWebsiteIds();

            // Iterate matched products and calculate discounts
            foreach ($products as $product) {
                foreach ($product->getWebsiteIds() as $storeId) {
                // Check product store id against store ids allowed in rule.
                if (!in_array($storeId, $ruleWebsiteIds)) {
                    continue;
                }

                // Check if store id exists. This is to ensure we don't face
                // issues because of corrupt data.
                try {
                    $this->storeManager->getStore($storeId);
                }
                catch (\Exception $e) {
                    $this->logger->warning('Junk store found in product.', [
                        $product->getId(),
                        $storeId,
                    ]);

                    continue;
                }

                if (isset($this->duplicatesCheck[$rule->getId()][$storeId][$product->getId()])) {
                    continue;
                }

                $this->duplicatesCheck[$rule->getId()][$storeId][$product->getId()] = 1;

                        $rows[] = [
                            'rule_id' => $rule->getId(),
                            'product_id' => $product->getId(),
                    // We don't use rule price for cart rules.
                    'rule_price' => 1,
                            'website_id' => $storeId,
                        ];

                        if (count($rows) >= $this->batchSize) {
                            $this->connection->insertMultiple(
                                $this->resource->getTableName('acq_salesrule_product'),
                                $rows
                            );

                            $rows = [];
                        }
                    }
                }

        if (!empty($rows)) {
            $this->connection->insertMultiple(
                $this->resource->getTableName('acq_salesrule_product'),
                $rows
            );
        }
    }

    /**
     * filterProducts.
     *
     * Filter product collection based on Product Conditions.
     *
     * @param $products
     * @param ProductCondition[] $conditions
     */
    protected function filterProducts($products, $conditions)
    {
        foreach ($conditions as $prodCond) {
            $attribute = $prodCond->getAttribute();
            $option = $prodCond->getOperatorForValidate();
            $value = $prodCond->getValueParsed();

            $comparisons = [
                '=='  => 'eq',
                '!='  => 'neq',
                '>'   => 'gt',
                '>='  => 'gteq',
                '<'   => 'lt',
                '<='  => 'lteq',
                '()'  => 'in',
                '!()' => 'nin',
                '{}'  => 'in',
                '!{}' => 'nin',
            ];

            if (isset($comparisons[$option])) {
                $compare = $comparisons[$option];
            } else {
                throw new \RuntimeException();
            }

            if ($attribute == 'category_ids') {
                $products->addCategoriesFilter([$compare => $value]);
            } else {
                $products->addAttributeToFilter(
                    $attribute, [$compare => $value]
                );
            }
        }
    }

    /**
     * locateProductConditions
     *
     * Traverse rule collection combinations and locate product specific
     * conditions to filter the product collection.
     *
     * @param array $combine Rule/Action Conditions
     *
     * @return ProductCondition[] $prodCond
     */
    protected function locateProductConditions($conditions)
    {
        $prodCond = [];

        foreach ($conditions as $cid => $condition) {
          // Get attribute code.
          $attribute_code = $condition->getData('attribute');
          if ($condition instanceof CombineCondition) {
                $prodCond = array_merge(
                    $prodCond,
                    $this->locateProductConditions($condition->getConditions())
                );
            } elseif ($condition instanceof ProductCondition
                && strpos($attribute_code, 'quote_item_') !== 0) {
                $prodCond[] = $condition;
            }
        }

        return ($prodCond);
    }

    /**
     * cleanAll
     *
     * Remove all saved index rows (truncate index).
     *
     * @return void
     */
    protected function cleanAll()
    {
        $this->connection->delete(
            $this->resource->getTableName('acq_salesrule_product')
        );
    }

    /**
     * cleanByProductIds
     *
     * Remove saved index rows by product ID.
     *
     * @param int[] $productIds Product IDS
     *
     * @return void
     */
    protected function cleanByProductIds(array $productIds)
    {
        $query = $this->connection->deleteFromSelect(
            $this->connection
                ->select()
                ->from($this->resource->getTableName('acq_salesrule_product'), 'product_id')
                ->distinct()
                ->where('product_id IN (?)', $productIds),
            $this->resource->getTableName('acq_salesrule_product')
        );

        $this->connection->query($query);
    }

    /**
     * cleanByRuleIds
     *
     * Remove saved index rows by rule ID.
     *
     * @param int[] $ruleIds Rule IDS
     *
     * @return void
     */
    protected function cleanByRuleIds(array $ruleIds)
    {
        $query = $this->connection->deleteFromSelect(
            $this->connection
                ->select()
                ->from($this->resource->getTableName('acq_salesrule_product'), 'rule_id')
                ->distinct()
                ->where('rule_id IN (?)', $ruleIds),
            $this->resource->getTableName('acq_salesrule_product')
        );

        $this->connection->query($query);
    }

    /**
     * getProductCollection
     *
     * Build a collection of enabled simple products to compare to
     * available rule conditions.
     *
     * @return ProductCollection $products
     */
    protected function getProductCollection()
    {
        $products = $this->productCollection->create();

        $products
            ->addAttributeToSelect('*')
            ->addAttributeToFilter('status', Status::STATUS_ENABLED);

        return ($products);
    }

    /**
     * getRuleCollection
     *
     * Build a collections of active sales (cart) rules to iterate and
     * match products to.
     *
     * @return RuleCollection $rules
     */
    protected function getRuleCollection()
    {
        $rules = $this->ruleCollection->create();

        $rules->addFieldToFilter('is_active', 1);

        return ($rules);
    }
}
