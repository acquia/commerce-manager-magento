<?php

/**
 * Acquia/CommerceManager/Model/TargetRuleProducts.php
 *
 * Acquia Commerce Manager Target Rule / Relation Products API Data Object
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Acquia\CommerceManager\Api\Data\TargetRuleProductsInterface;

/**
 * TargetRuleProducts
 *
 * Acquia Commerce Manager Target Rule / Relation Products API Data Object
 */
class TargetRuleProducts
    // consider extending \Magento\Framework\Model\AbstractExtensibleModel
    extends \Magento\Framework\Model\AbstractExtensibleModel
    //extends \Magento\Framework\Api\AbstractExtensibleObject
    implements TargetRuleProductsInterface
{
    /**
     * Prefix of model events names
     *
     * @var string
     */
    protected $_eventPrefix = 'acquia_commercemanager_target_rule_products';

  /**
   * Constructor
   *
   * @param \Magento\Framework\Model\Context $context
   * @param \Magento\Framework\Registry $registry
   * @param \Magento\Framework\Api\ExtensionAttributesFactory $extensionFactory
   * @param \Magento\Framework\Api\AttributeValueFactory $customAttributeFactory
   * @param array $data
   */
    public function __construct(
        \Magento\Framework\Model\Context $context,
        \Magento\Framework\Registry $registry,
        \Magento\Framework\Api\ExtensionAttributesFactory $extensionFactory,
        \Magento\Framework\Api\AttributeValueFactory $customAttributeFactory,
        array $data = []
    ) {
        parent::__construct(
            $context,
            $registry,
            $extensionFactory,
            $customAttributeFactory,
            null,
            null,
            $data
        );
    }

  /**
   * afterGetRelatedProductsByType
   *
   * Fires an event
   */
    public function afterGetRelatedProductsByType()
    {
        $this->_eventManager->dispatch(
            $this->_eventPrefix.'_after_get_related_products_by_type',
            [$this->_eventObject => $this]
        );
    }

    /**
     * getCrosssell
     *
     * Product by target rule type 'crosssell'
     *
     * @return string[] $crosssell
     */
    public function getCrosssell()
    {
        return $this->getData(self::CROSS_SELL);
    }

    /**
     * setCrosssell
     *
     * @param string[] $crosssell
     * @return $this
     */
    public function setCrosssell($crosssell)
    {
        return $this->setData(self::CROSS_SELL, $crosssell);
    }

    /**
     * getRelated
     *
     * Product by target rule type 'related'
     *
     * @return string[] $related
     */
    public function getRelated()
    {
        return $this->getData(self::RELATED);
    }

    /**
     * setRelated
     *
     * @param string[] $related
     * @return $this
     */
    public function setRelated($related)
    {
        return $this->setData(self::RELATED, $related);
    }

    /**
     * getUpsell
     *
     * Product by target rule type 'upsell'
     *
     * @return string[] $upsell
     */
    public function getUpsell()
    {
        return $this->getData(self::UP_SELL);
    }

    /**
     * setUpsell
     *
     * @param string[] $upsell
     * @return $this
     */
    public function setUpsell($upsell)
    {
        return $this->setData(self::UP_SELL, $upsell);
    }

    /**
     * getSku
     *
     * @inheritdoc
     */
    public function getSku()
    {
        return $this->getData(self::SKU);
    }

    /**
     * setSku
     *
     * @inheritdoc
     * @param string $sku
     * @return $this
     */
    public function setSku($sku)
    {
        return $this->setData(self::SKU, $sku);
    }

    /**
     * getType
     *
     * @inheritdoc
     */
    public function getType()
    {
        return $this->getData(self::TYPE);
    }

    /**
     * setType
     *
     * @inheritdoc
     * @param string $type
     * @return $this
     */
    public function setType($type)
    {
        return $this->setData(self::TYPE, $type);
    }

    /**
     * {@inheritdoc}
     *
     * @return \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface
     */
    public function getExtensionAttributes()
    {
        $extensionAttributes = $this->_getExtensionAttributes();
        if (null === $extensionAttributes) {
            /** @var \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface $extensionAttributes */
            $extensionAttributes = $this->extensionAttributesFactory->create(TargetRuleProductsInterface::class);
            $this->setExtensionAttributes($extensionAttributes);
        }
        return $extensionAttributes;
    }


    /**
     * {@inheritdoc}
     *
     * @param \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface $extensionAttributes
     * @return $this
     */
    public function setExtensionAttributes(\Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface $extensionAttributes)
    {
        return $this->_setExtensionAttributes($extensionAttributes);
    }
}
