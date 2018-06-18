<?php

/**
 * Acquia/CommerceManager/Model/CartWithTotals.php
 *
 * Acquia Commerce Manager Combined Cart Totals Entity
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Acquia\CommerceManager\Api\Data\CartWithTotalsInterface;
use Magento\Quote\Api\Data\CartInterface;
use Magento\Quote\Api\Data\TotalsInterface;

/**
 * CartWithTotals
 *
 * Acquia Commerce Manager Combined Cart Totals Entity
 */
class CartWithTotals implements CartWithTotalsInterface
{
    /**
     * Quote Entity
     * @var \Magento\Quote\Api\Data\CartInterface $quote
     */
    protected $quote;

    /**
     * Quote Totals Entity
     * @var \Magento\Quote\Api\Data\TotalsInterface $totals
     */
    protected $totals;

    /**
     * The response message info.
     * @var array
     */
    protected $responseMessage = [];

    /**
     * Constructor
     *
     * @param CartInterface $quote
     * @param TotalsInterface|null $totals
     * @param array $responseMessage
     */
    public function __construct(
        CartInterface $quote,
        TotalsInterface $totals = null,
        $responseMessage = []
    ) {
        $this->quote = $quote;
        $this->totals = $totals;
        $this->responseMessage = $responseMessage;
    }

    /**
     * {@inheritdoc}
     */
    public function getCart()
    {
        return ($this->quote);
    }

    /**
     * {@inheritdoc}
     */
    public function getTotals()
    {
        return ($this->totals);
    }

    /**
     * {@inheritdoc}
     */
    public function getResponseMessage()
    {
        return($this->responseMessage);
    }

}
