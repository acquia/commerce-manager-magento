<?php

/**
 * Acquia/CommerceManager/Api/Data/ExtendedCatalogRuleInterface.php
 *
 * Acquia Commerce Manager Extended Catalog Rule API Data Object
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api\Data;

use Magento\CatalogRule\Api\Data\RuleInterface;

/**
 * ExtendedCatalogRuleInterface
 *
 * Acquia Commerce Manager Extended Catalog Rule API Data Object
 */
interface ExtendedCatalogRuleInterface extends RuleInterface
{
    /**
     * getProductDiscounts
     *
     * Get an array of calculated / indexed matching products
     * and discount amounts for this catalog rule.
     *
     * @return array $discounts
     */
    public function getProductDiscounts();

    /**
     * setProductDiscounts
     *
     * Set the available product discounts for this rule.
     *
     * @param array $discounts
     *
     * @return self $this
     */
    public function setProductDiscounts(array $discounts);
}
