<?php

/**
 * Acquia/CommerceManager/Observer/CustomerDeleteObserver.php
 *
 * Acquia Commerce Connector Customer Delete Observer
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Observer;

use Magento\Framework\Event\Observer;
use Magento\Framework\Event\ObserverInterface;
use Magento\Customer\Model\Backend\Customer\Interceptor as CustomerInterceptor;

class CustomerDeleteObserver extends ConnectorObserver implements ObserverInterface
{
    /**
     * Connector Customer Delete Endpoint
     * @const ENDPOINT_CUSTOMER_DELETE
     */
    const ENDPOINT_CUSTOMER_DELETE = 'ingest/customer/delete';

    /**
     * When deleting customer, delete also from the Drupal.
     *
     * @param \Magento\Framework\Event\Observer $observer
     * @return void
     */
    public function execute(Observer $observer)
    {
        /* @var \Magento\Customer\Model\Customer $customer */
        $customer = $observer->getEvent()->getCustomer();
        $storeId = $customer->getStoreId();
        $this->logger->notice(
            sprintf(
              'CustomerDeleteObserver: deleting Customer id %d and email %s.',
              $customer->getId(),
              $customer->getEmail()
            ),
            [ 'id' => $customer->getId(), 'store_id' => $storeId]
        );

        $edata = [
            'email' =>  $customer->getEmail(),
        ];
        $doReq = function ($client, $opt) use ($edata) {
            $opt['json'] = $edata;
            return $client->post(self::ENDPOINT_CUSTOMER_DELETE, $opt);
        };

        $this->tryRequest($doReq, 'CustomerDeleteObserver', $storeId);
    }
}
