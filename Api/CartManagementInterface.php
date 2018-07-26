<?php

/**
 * Acquia/CommerceManager/Api/CartManagementInterface.php
 *
 * Acquia Commerce Manager Cart Api Extended Operations
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api;

use Magento\Quote\Api\Data\CartItemInterface;
use Magento\Quote\Api\Data\AddressInterface;
use Magento\Checkout\Api\Data\ShippingInformationInterface;
use Magento\Quote\Api\Data\PaymentInterface;
use Magento\Quote\Api\Data\CartInterface;

/**
 * CartManagementInterface
 *
 * Acquia Commerce Manager Cart Api Extended Operations
 * @api
 */
interface CartManagementInterface
{
    /**
     * Unassociates all carts that are currently attached to a customer.
     *
     * @param int $customerId
     *
     * @return bool $success
     */
    public function abandon($customerId);

    /**
     * Associate a new cart to a customer (and abandon any existing
     * cart), optionally with a coupon code to apply.
     *
     * This combines multiple operations to a single endpoint for UI
     * performance.
     *
     * @param int $customerId Customer ID
     * @param int $cartId Cart ID
     * @param int $storeId Store ID
     * @param string $couponCode Cart coupon code
     *
     * @return boolean $couponApplied
     */
    public function associateCart(
        $customerId,
        $cartId,
        $storeId = null,
        $couponCode = null
    );

    /**
     * Updates the specified cart with optional items, billing address,
     * shipping address / method, payment method, and coupon code.
     *
     * @param int $cartId
     * @param \Magento\Quote\Api\Data\CartItemInterface[] $items
     * @param \Magento\Quote\Api\Data\AddressInterface $billing
     * @param \Magento\Checkout\Api\Data\ShippingInformationInterface $shipping
     * @param \Magento\Quote\Api\Data\PaymentInterface $method
     * @param string $coupon
     * @param mixed $extension
     *
     * @return \Acquia\CommerceManager\Api\Data\CartWithTotalsInterface
     */
    public function updateCart(
        $cartId,
        $items = [],
        AddressInterface $billing = null,
        ShippingInformationInterface $shipping = null,
        PaymentInterface $payment = null,
        $coupon = null,
        $extension = []
    );

    /**
     * Get a cart from its cart ID.
     *
     * @param int $cartId
     *
     * @return \Acquia\CommerceManager\Api\Data\CartWithTotalsInterface
     */
    public function getCart(
        $cartId
    );
}
