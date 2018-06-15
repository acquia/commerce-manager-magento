<?php

/**
 * Acquia/CommerceManager/Api/Data/CartWithTotalsInterface.php
 *
 * Acquia Commerce Manager Cart With Totals API Data Object
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api\Data;

use Magento\Quote\Api\Data\CartInterface;
use Magento\Quote\Api\Data\TotalsInterface;

/**
 * CartWithTotalsInterface
 *
 * Acquia Commerce Manager Cart With Totals API Data Object
 *
 * @api
 */
interface CartWithTotalsInterface
{
    /**
     * getCart
     *
     * Get the Cart / Quote object updated / loaded.
     *
     * @return \Acquia\CommerceManager\Api\Data\ExtendedCartInterface
     */
    public function getCart();

    /**
     * getTotals
     *
     * Get the calculated cart totals.
     *
     * @return \Magento\Quote\Api\Data\TotalsInterface
     */
    public function getTotals();

    /**
     * getResponseMessage
     *
     * Get the response message info.
     *
     * @return array
     */
    public function getResponseMessage();
}
