<?php

/**
 * Acquia/CommerceManager/Model/Converter/ToSalesRuleExtendedDataModel.php
 *
 * Acquia Commerce Manager Extended Sales / Cart Rule Data Model Converter
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Converter;

use Acquia\CommerceManager\Api\Data\ExtendedSalesRuleInterfaceFactory;
use Magento\SalesRule\Model\Converter\ToDataModel;
use Magento\SalesRule\Api\Data\RuleExtensionFactory;

/**
 * ToSalesRuleExtendedDataModel
 *
 * Acquia Commerce Manager Extended Sales / Cart Rule Data Model Converter
 */
class ToSalesRuleExtendedDataModel extends ToDataModel
{

    /**
     * @var RuleExtensionFactory
     */
    private $extensionFactory;

    /**
     * Extended Sales / Cart Rule Api Data Object Factory
     * @var ExtendedSalesRuleInterfaceFactory $extendedRuleDataFactory
     */
    protected $extendedRuleDataFactory;

    /**
     * @param ExtendedSalesRuleInterfaceFactory $extendedRuleDataFactory
     * @param \Magento\SalesRule\Model\RuleFactory $ruleFactory
     * @param \Magento\SalesRule\Api\Data\RuleInterfaceFactory $ruleDataFactory
     * @param \Magento\SalesRule\Api\Data\ConditionInterfaceFactory $conditionDataFactory
     * @param \Magento\SalesRule\Api\Data\RuleLabelInterfaceFactory $ruleLabelFactory
     * @param \Magento\Framework\Reflection\DataObjectProcessor $dataObjectProcessor
     * @param RuleExtensionFactory|null $extensionFactory
     */
    public function __construct(
        ExtendedSalesRuleInterfaceFactory $extendedRuleDataFactory,
        \Magento\SalesRule\Model\RuleFactory $ruleFactory,
        \Magento\SalesRule\Api\Data\RuleInterfaceFactory $ruleDataFactory,
        \Magento\SalesRule\Api\Data\ConditionInterfaceFactory $conditionDataFactory,
        \Magento\SalesRule\Api\Data\RuleLabelInterfaceFactory $ruleLabelFactory,
        \Magento\Framework\Reflection\DataObjectProcessor $dataObjectProcessor,
        RuleExtensionFactory $extensionFactory = null
    ) {
        $this->extendedRuleDataFactory = $extendedRuleDataFactory;
        // This will fail 'level 0' (strictest) coding standards
        // but we are mimicking the parent class here.
        $this->extensionFactory = $extensionFactory ?:
            \Magento\Framework\App\ObjectManager::getInstance()->get(RuleExtensionFactory::class);


        return (parent::__construct(
            $ruleFactory,
            $ruleDataFactory,
            $conditionDataFactory,
            $ruleLabelFactory,
            $dataObjectProcessor,
            $extensionFactory
        ));
    }

    /**
     * {@inheritDoc}
     */
    public function toDataModel(\Magento\SalesRule\Model\Rule $rule)
    {
        $data = $rule->getData();
        $data = $this->convertExtensionAttributesToObject($data);

        /** @var \Acquia\CommerceManager\Model\Data\ExtendedSalesRule $data */
        $data = $this->extendedRuleDataFactory->create([
            'data' => $data,
        ]);

        $this->mapFields($data, $rule);

        return ($data);
    }

    /**
     * Convert extension attributes of model to object if it is an array
     * Code repetition. See parent. Necessary due to parent is private.
     *
     * @param array $data
     * @return array
     */
    private function convertExtensionAttributesToObject(array $data)
    {
        if (isset($data['extension_attributes']) && is_array($data['extension_attributes'])) {
            /** @var RuleExtensionInterface $attributes */
            $data['extension_attributes'] = $this->extensionFactory->create(['data' => $data['extension_attributes']]);
        }
        return $data;
    }

}
