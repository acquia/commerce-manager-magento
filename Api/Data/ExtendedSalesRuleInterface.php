<?php

/**
 * Acquia/CommerceManager/Api/Data/ExtendedSalesRuleInterface.php
 *
 * Acquia Commerce Manager Extended Sales Rule Rule API Data Object
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api\Data;

use Magento\SalesRule\Api\Data\RuleInterface;

/**
 * ExtendedSalesRuleInterface
 *
 * Acquia Commerce Manager Extended Sales Rule or Cart Rule API Data Object
 */
interface ExtendedSalesRuleInterface extends RuleInterface
{
    /**
     * getProductDiscounts
     *
     * Get an array of calculated / indexed matching products and
     * discount amounts for this sales rule.
     *
     * @return float[] $discounts
     */
    public function getProductDiscounts();

    /**
     * setProductDiscounts
     *
     * Set the available product discount amounts for this rule.
     *
     * @param float[] $discounts
     *
     * @return self $this
     */
    public function setProductDiscounts(array $discounts);
}
