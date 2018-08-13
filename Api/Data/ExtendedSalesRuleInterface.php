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
     * @return array $discounts
     */
    public function getProductDiscounts();

    /**
     * setProductDiscounts
     *
     * Set the available product discount amounts for this rule.
     *
     * @param array $discounts
     *
     * @return self $this
     */
    public function setProductDiscounts(array $discounts);

    /**
     * getCouponCode
     *
     * Get coupon code for the rule.
     *
     * @return String[] $coupon_code
     */
    public function getCouponCode();

    /**
     * setCouponCode
     *
     * Set the coupon code for this rule.
     *
     * @param String[] $coupon_code
     *
     * @return self $this
     */
    public function setCouponCode(Array $coupon_code);
}
