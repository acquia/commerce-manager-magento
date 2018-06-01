<?php

/**
 * Model/CatalogRuleRepository.php
 *
 * Catalog Price Rule API Repository
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Acquia\CommerceManager\Model\ResourceModel\Rule\CollectionFactory as RuleCollectionFactory;
use Magento\Eav\Model\Config as EavConfig;
use Magento\Framework\Api\ExtensionAttribute\JoinProcessorInterface;
use Magento\Framework\EntityManager\MetadataPool;
use Magento\Framework\Pricing\PriceCurrencyInterface;
use Magento\Store\Model\StoreManagerInterface;
use Psr\Log\LoggerInterface;

/**
 * CatalogRuleRepository
 *
 * Catalog Price Rule API Repository
 */
class CatalogRuleRepository
    extends \Magento\CatalogRule\Model\CatalogRuleRepository
    implements \Acquia\CommerceManager\Api\CatalogRuleRepositoryInterface
{
    /**
     * @var RuleCollectionFactory
     */
    protected $ruleCollectionFactory;

    /**
     * @var EavConfig
     */
    protected $eavConfig;

    /**
     * Magento Entity Extension Processor
     *
     * @var JoinProcessorInterface
     */
    protected $extensionAttributesJoinProcessor;

    /**
     * @var MetadataPool
     */
    protected $metadataPool;

    /**
     * @var PriceCurrencyInterface
     */
    protected $priceCurrency;

    /**
     * @var StoreManagerInterface
     */
    protected $storeManager;

    /**
     * @var LoggerInterface
     */
    Protected $logger;

    /**
     * CatalogRuleRepository constructor.
     *
     * @param RuleCollectionFactory $ruleCollectionFactory
     * @param EavConfig $eavConfig
     * @param JoinProcessorInterface $extensionAttributesJoinProcessor
     * @param MetadataPool $metadataPool
     * @param PriceCurrencyInterface $priceCurrency
     * @param StoreManagerInterface $storeManager
     * @param LoggerInterface $logger
     * @param \Magento\CatalogRule\Model\ResourceModel\Rule $ruleResource
     * @param \Magento\CatalogRule\Model\RuleFactory $ruleFactory
     */
    public function __construct(
        RuleCollectionFactory $ruleCollectionFactory,
        EavConfig $eavConfig,
        JoinProcessorInterface $extensionAttributesJoinProcessor,
        MetadataPool $metadataPool,
        PriceCurrencyInterface $priceCurrency,
        StoreManagerInterface $storeManager,
        LoggerInterface $logger,
        \Magento\CatalogRule\Model\ResourceModel\Rule $ruleResource,
        \Magento\CatalogRule\Model\RuleFactory $ruleFactory
    ) {
        $this->ruleCollectionFactory = $ruleCollectionFactory;
        $this->eavConfig = $eavConfig;
        $this->extensionAttributesJoinProcessor = $extensionAttributesJoinProcessor;
        $this->metadataPool = $metadataPool;
        $this->priceCurrency = $priceCurrency;
        $this->storeManager = $storeManager;
        $this->logger = $logger;
        parent::__construct(
            $ruleResource,
            $ruleFactory
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getCount()
    {
        $collection = $this->ruleCollectionFactory->create()->addFieldToFilter('is_active', 1);
        return ((int)$collection->getSize());
    }

    /**
     * {@inheritDoc}
     */
    public function getList($pageSize = 0, $pageCount = 0)
    {

        /* @var \Acquia\CommerceManager\Model\ResourceModel\Rule\Collection $collection */
        $collection = $this->ruleCollectionFactory->create()->addFieldToFilter('is_active', 1);

        $website_id = $this->storeManager->getWebsite()->getId();
        $collection->addWebsiteFilter($website_id);

        $this->extensionAttributesJoinProcessor->process(
            $collection,
            \Magento\CatalogRule\Api\Data\RuleInterface::class
        );

        if ($pageSize) {
            $collection->setPageSize($pageSize);
        }

        if ($pageCount) {
            $collection->setCurPage($pageCount);
        }

        // Load product discount prices from matched rules.
        $prices = $this->loadRulePrices($collection->getAllIds());

        $rulesArray = [];
        /* @var \Magento\CatalogRule\Model\Rule $rule */
        foreach ($collection as $rule) {
            $ruleId = $rule->getId();
            $this->get($ruleId);
            // Catalog rules don't have store labels
            // $rule->getStoreLabels();

            // catalog rules don't have a data converter.
            // $result = $this->toDataModelConverter->toDataModel($rule);
            // so just role with it
            $ruleArray = $rule->toArray();
            $ruleArray['product_discounts'] = [];
            if (isset($prices[$ruleId]))
            {
                $ruleArray['product_discounts'] = $prices[$ruleId];
            }
            $rulesArray[] = $ruleArray;
        }

        return ($rulesArray);
    }

    /**
     * loadRulePrices
     *
     * Load indexed rule products and calculate discounts by rule IDs.
     *
     * @param int[] $ruleIds
     *
     * @return array $prices
     */
    protected function loadRulePrices(array $ruleIds)
    {
        $prices = [];

        if (empty($ruleIds)) {
            return ($prices);
        }

        $websiteId = $this->storeManager->getWebsite()->getId();
        $storeId = $this->storeManager->getStore()->getId();
        $select = $this->getRulesPriceStmt($websiteId, $ruleIds);

        while ($ruleData = $select->fetch()) {
            if (isset($ruleData['rule_id']) && ($rid = $ruleData['rule_id'])) {
                $discount = $this->calcRuleProductDiscount($ruleData);
                $prodPrice = [
                    'product_id' => (integer) $ruleData['product_id'],
                    'product_sku' => (string) $ruleData['product_sku'],
                    'rule_price' => (string) $discount,
                    'customer_group_id' => (integer) $ruleData['customer_group_id'],
                    'website_id' => (integer) $websiteId,
                    'store_id' => (integer) $storeId,
                ];

                if (isset($prices[$rid])) {
                    $prices[$rid][] = $prodPrice;
                } else {
                    $prices[$rid] = [$prodPrice];
                }
            }
        }

        return ($prices);
    }

    /**
     * calcRuleProductDiscount
     *
     * Calculate a product discount amount based on price and rule data
     * loaded from the catalog rule product index.
     *
     * This differs from the native Magento calculation functionality
     * in that instead of calculating final cart item price we want
     * to calculate the discount amount.
     *
     * @param string[] $ruleData
     *
     * @return float
     */
    protected function calcRuleProductDiscount(array $ruleData)
    {
        $discount = 0;
        $productPrice =
            (isset($ruleData['default_price'])) ?
                $ruleData['default_price'] :
                0;

        switch ($ruleData['action_operator']) {
            case 'to_fixed':
                $discount = $productPrice - min($ruleData['action_amount'], $productPrice);
                break;
            case 'to_percent':
                $discount = $productPrice - ($productPrice * $ruleData['action_amount'] / 100);
                break;
            case 'by_fixed':
                $discount = min($productPrice, $ruleData['action_amount']);
                break;
            case 'by_percent':
                $discount = $productPrice - ($productPrice * (1 - $ruleData['action_amount'] / 100));
                break;
            default:
                $discount = 0;
        }

        return ($this->priceCurrency->round($discount));
    }

    /**
     * getRulesPriceStmt
     *
     * Build a product / price select statement for products matching
     * catalog rules by website ID.
     *
     * @param int $websiteId
     * @param int[] $ruleIds
     *
     * @return \Zend_Db_Statement_Interface
     */
    protected function getRulesPriceStmt($websiteId, $ruleIds)
    {
        /* @var \Acquia\CommerceManager\Model\ResourceModel\Rule\Collection $collection */
        $collection = $this->ruleCollectionFactory->create();
        /* @var \Magento\Framework\DB\Select $select */
        $select = $collection->getConnection()->select()
            ->from(['rp' => $collection->getTable('catalogrule_product')])
            ->where('rp.rule_id IN (?)', $ruleIds)
            ->order([
                'rp.product_id',
                'rp.website_id',
                'rp.customer_group_id',
                'rp.sort_order',
                'rp.rule_id'
            ]);

        $priceAttr = $this->eavConfig->getAttribute(\Magento\Catalog\Model\Product::ENTITY, 'price');
        $priceTable = $priceAttr->getBackend()->getTable();
        $attributeId = $priceAttr->getId();

        $linkField = $this->metadataPool
            ->getMetadata(\Magento\Catalog\Api\Data\ProductInterface::class)
            ->getLinkField();

        $select->join(
            ['e' => $collection->getTable('catalog_product_entity')],
            'e.entity_id = rp.product_id',
            ['product_sku' => 'e.sku']
        );

        $joinCondition =
            '%1$s.' . $linkField . '= e.' . $linkField .
            ' AND (%1$s.attribute_id =' . $attributeId . ')' .
            ' AND (%1$s.store_id = %2$s)';

        $select->join(
            ['pp_default' => $priceTable],
            sprintf(
                $joinCondition,
                'pp_default',
                \Magento\Store\Model\Store::DEFAULT_STORE_ID
            ),
            []
        );

        $website = $this->storeManager->getWebsite($websiteId);
        $defaultGroup = $website->getDefaultGroup();
        if ($defaultGroup instanceof \Magento\Store\Model\Group) {
            $storeId = $defaultGroup->getDefaultStoreId();
        } else {
            $storeId = \Magento\Store\Model\Store::DEFAULT_STORE_ID;
        }

        $select->joinInner(
            ['product_website' => $collection->getTable('catalog_product_website')],
            'product_website.product_id = rp.product_id ' .
            'AND product_website.website_id = rp.website_id ' .
            'AND product_website.website_id = ' . $websiteId,
            []
        );

        $tableAlias = 'pp' . $websiteId;

        $select->joinLeft(
            [$tableAlias => $priceTable],
            sprintf($joinCondition, $tableAlias, $storeId),
            []
        );

        $defPriceSql = $collection->getConnection()->getIfNullSql(
            $tableAlias . '.value',
            'pp_default.value'
        );

        $select->columns([
            'default_price' => $defPriceSql,
        ]);

        return ($collection->getConnection()->query($select));
    }
}
