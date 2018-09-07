<?php

/**
 * Acquia/CommerceManager/Api/Data/TargetRuleProductsInterface.php
 *
 * Acquia Commerce Manager Target Rule / Relation Products API Data Object
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api\Data;

use \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface as ExtensionInterface;
/**
 * TargetRuleProductsInterface
 *
 * Acquia Commerce Manager Target Rule / Relation Products API Data Object
 */
interface TargetRuleProductsInterface
    extends \Magento\Framework\Api\ExtensibleDataInterface
{
    const CROSS_SELL = "crosssell";
    const UP_SELL = "upsell";
    const RELATED = "related";
    const SKU = "sku";
    const TYPE = "type";

    /**
     * getCrosssell
     *
     * Product by target rule type 'crosssell'
     *
     * @return string[] $crosssell
     */
    public function getCrosssell();

    /**
     * setCrosssell
     *
     * @param string[] $crosssell
     * @return $this
     */
    public function setCrosssell($crosssell);

    /**
     * getRelated
     *
     * Product by target rule type 'related'
     *
     * @return string[] $related
     */
    public function getRelated();

    /**
     * setRelated
     *
     * @param string[] $related
     * @return $this
     */
    public function setRelated($related);

    /**
     * getUpsell
     *
     * Product by target rule type 'upsell'
     *
     * @return string[] $upsell
     */
    public function getUpsell();

    /**
     * setUpsell
     *
     * @param string[] $upsell
     * @return $this
     */
    public function setUpsell($upsell);

    /**
     * getSku
     *
     * The sku that has these product relations (due to the target rule)
     * @return string
     */
    public function getSku();

    /**
     * setSku
     *
     * @param string $sku
     * @return $this
     */
    public function setSku($sku);

    /**
     * getType
     *
     * The product relation type (upsell, crosssell, related, all, extension)
     * @return string
     */
    public function getType();

    /**
     * setType
     *
     * @param string $type
     * @return $this
     */
    public function setType($type);

    /**
     * Retrieve existing extension attributes object or create a new one.
     *
     * @return \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface|null
     */
    public function getExtensionAttributes();

    /**
     * Set an extension attributes object.
     *
     * @param ExtensionInterface $extensionAttributes
     * @return $this
     */
    public function setExtensionAttributes(ExtensionInterface $extensionAttributes);


}
