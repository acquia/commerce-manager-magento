<?php

namespace Acquia\CommerceManager\Plugin;

class QuoteManagementPlugin
{
    /** @var \Magento\Framework\App\State */
    private $appState;
    /** @var \Magento\Quote\Api\PaymentMethodManagementInterface */
    private $paymentMethodManager;

    public function __construct(
        \Magento\Framework\App\State $appState,
        \Magento\Quote\Api\PaymentMethodManagementInterface $paymentMethodManager
    )
    {
        $this->appState = $appState;
        $this->paymentMethodManager = $paymentMethodManager;
    }

    /**
     * We want to ensure a payment method is always present
     * before placeOrder is called.
     *
     * You might ask why this is necessary. A cursory glance through the code will
     * identify a number of different processes for placing orders. The Magneto API
     * calls this placeOrder function which has some code-repetition from
     * \Magento\Quote\Model\PaymentMethodManagement::set() but the code is not identical.
     * We see also \Magento\Quote\Model\QuoteManagement::submit() which seems more
     * appropriate for a quote that has previously had a payment method applied but
     * there is no current API route to it.
     *
     * Here we simply fetch the payment method and pass it on to placeOrder()
     *
     * @param \Magento\Customer\Api\CustomerRepositoryInterface|\Magento\Quote\Api\CartManagementInterface $subject
     * @param $cartId Int
     * @param \Magento\Quote\Api\Data\PaymentInterface|null $paymentMethod
     * @return array
     * @internal param \Magento\Framework\Api\SearchCriteriaInterface $searchCriteria
     */
    public function beforePlaceOrder(
        \Magento\Quote\Api\CartManagementInterface $subject,
        $cartId,
        \Magento\Quote\Api\Data\PaymentInterface $paymentMethod = null
    )
    {
        // @TODO (Malachy): Add 'If module is enabled'

        // Restrict this plugin to restAPI only
        $notRestApi = !($this->appState->getAreaCode() == \Magento\Framework\App\Area::AREA_WEBAPI_REST);
        if ($notRestApi) {
            return [$cartId, $paymentMethod];
        }

        if (!$paymentMethod){
            /* @var \Magento\Quote\Api\Data\PaymentInterface */
            $paymentMethod = $this->paymentMethodManager->get($cartId);
        }

        return [$cartId, $paymentMethod];
    }

}
