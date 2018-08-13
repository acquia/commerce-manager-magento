<?php

/**
 * Acquia/CommerceManager/Model/SalesRuleRepository.php
 *
 * Acquia Commerce Manager Sales / Cart Rule Extended Repository
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Magento\SalesRule\Model\ResourceModel\Coupon\CollectionFactory as CouponCollectionFactory;
use Acquia\CommerceManager\Api\SalesRuleRepositoryInterface;
use Acquia\CommerceManager\Model\Converter\ToSalesRuleExtendedDataModel as ToDataModel;
use Magento\SalesRule\Model\ResourceModel\Rule\CollectionFactory;
use Magento\Framework\Api\ExtensionAttribute\JoinProcessorInterface;
use Magento\Framework\App\ResourceConnection;
use Magento\Store\Model\StoreManagerInterface;

/**
 * SalesRuleRepository
 *
 * Acquia Commerce Manager Sales / Cart Rule Extended Repository
 */
class SalesRuleRepository implements SalesRuleRepositoryInterface
{
    /**
     * Magento Database Connection
     * @var AdapterInterface $connection
     */
    protected $connection;

    /**
     * Cached storage of product discounts by rule ID.
     * @var array $discountData
     */
    protected $discountData;

    /**
     * Magento Entity Extension Processor
     * @var JoinProcessorInterface $extensionAttributesJoinProcessor
     */
    protected $extensionAttributesJoinProcessor;

    /**
     * Magento Database Resource
     * @var ResourceConnection $resource
     */
    protected $resource;

    /**
     * Sales Rule Collection Factory
     * @var CollectionFactory $ruleCollectionFactory
     */
    protected $ruleCollectionFactory;

    /**
     * Magento Data Object Converter
     * @var ToDataModel $toDataModelConverter
     */
    protected $toDataModelConverter;

    /**
     * @var StoreManagerInterface $storeManager
     */
    private $storeManager;

    /**
     * @var \Magento\SalesRule\Model\ResourceModel\Coupon\CollectionFactory
     */
    protected $_couponCollectionFactory;

    /**
     * Constructor
     *
     * @param CollectionFactory $ruleCollectionFactory
     * @param ResourceConnection $resource
     * @param ToDataModel $toDataModelConverter
     * @param JoinProcessorInterface $extensionAttributesJoinProcessor
     * @param StoreManagerInterface $storeManager
     */
    public function __construct(
        CouponCollectionFactory $couponCollectionFactory,
        CollectionFactory $ruleCollectionFactory,
        ResourceConnection $resource,
        ToDataModel $toDataModelConverter,
        JoinProcessorInterface $extensionAttributesJoinProcessor,
        StoreManagerInterface $storeManager
    ) {
        $this->_couponCollectionFactory = $couponCollectionFactory;
        $this->ruleCollectionFactory = $ruleCollectionFactory;
        $this->resource = $resource;
        $this->connection = $resource->getConnection();
        $this->toDataModelConverter = $toDataModelConverter;
        $this->extensionAttributesJoinProcessor = $extensionAttributesJoinProcessor;
        $this->storeManager = $storeManager;
    }

    /**
     * {@inheritDoc}
     */
    public function getList($ruleId = 0, $productId = 0)
    {
        // Load Sales Rule / Product index data
        $this->loadProductDiscounts($ruleId, $productId);

        // Create Sales Rule collection
        $rules = $this->ruleCollectionFactory->create();
        $this->extensionAttributesJoinProcessor->process(
            $rules,
            \Magento\SalesRule\Api\Data\RuleInterface::class
        );

        if ((int)$ruleId > 0) {
            $rules->addFieldToFilter('rule_id', (int)$ruleId);
        } else {
            $rules->addFieldToFilter('is_active', 1);
            $website_id = $this->storeManager->getWebsite()->getId();
            $rules->addWebsiteFilter($website_id);
        }

        $rules->load();

        $results = [];

        // Load / convert to data models and add discount data
        /** @var \Magento\SalesRule\Model\Rule $rule */
        foreach ($rules as $rule) {
            $rid = $rule->getId();
            $rule->load($rid);
            $rule->getStoreLabels();
            $result = $this->toDataModelConverter->toDataModel($rule);
            $discounts =
                (isset($this->discountData[$rid])) ?
                    $this->discountData[$rid] :
                    [];
            $coupon_codes = [];
            if ($rule->getCouponCode()){
                foreach($rule->getCoupons() as $coupon){
                    $coupon_codes[]=$coupon->getCode();
                }
            } else if ($rule->getUseAutoGeneration() > 0) {
                $couponCollection = $this->_couponCollectionFactory->create();
                $couponCollection->addRuleToFilter($rid);
                $couponCollection->addGeneratedCouponsFilter();
                foreach($couponCollection->getItems() as $coupon){
                    $coupon_codes[] = $coupon->getCode();
                }
            }

            $result->setCouponCode($coupon_codes);
            $result->setProductDiscounts($discounts);
            $results[] = $result;
        }

        return ($results);
    }

    /**
     * loadProductDiscounts
     *
     * Load the indexed product discounts into the cached storage
     * keyed by rule Id, optionally filtered by rule and product ids.
     *
     * @param int $ruleId Rule Id to filter
     * @param int $productId Product Id to filter
     *
     * @return void
     */
    protected function loadProductDiscounts($ruleId, $productId)
    {
        $select = $this->connection
            ->select()
            ->from(['asp' => $this->resource->getTableName('acq_salesrule_product')]);

        if ($ruleId > 0) {
            $select->where('asp.rule_id = ?', $ruleId);
        }

        if ($productId > 0) {
            $select->where('asp.product_id = ?', $productId);
        }

        // Always load data for store currently in URL context.
        $websiteId = $this->storeManager->getStore()->getWebsiteId();
        $select->where('asp.website_id = ?', $websiteId);

        $select->join(
            ['e' => $this->resource->getTableName('catalog_product_entity')],
            'e.entity_id = asp.product_id',
            ['product_sku' => 'e.sku']
        );

        // I suppose we are not expecting an enormous amount of promotions.
        // Maybe we would also filter on 'enabled' promotions.
        // But this way we can see future promotions
        $rows = $this->connection->fetchAll($select);

        foreach ($rows as $row) {
            $rid = $row['rule_id'];
            if ($rid) {
                if (isset($this->discountData[$rid])) {
                    $this->discountData[$rid][] = $row;
                } else {
                    $this->discountData[$rid] = [$row];
                }
            }
        }
    }
}
