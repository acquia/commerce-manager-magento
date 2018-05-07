<?php

/**
 * Acquia/CommerceManager/Observer/CategorySaveObserver.php
 *
 * Acquia Commerce Connector Category Save Observer
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Observer;

use Magento\Framework\Event\ObserverInterface;
use Magento\Framework\Event\Observer;

/**
 * CategorySaveObserver
 *
 * Acquia Commerce Connector Category Save Observer
 */
class CategorySaveObserver extends CategoryObserver implements ObserverInterface
{
    /**
     * execute
     *
     * Send updated category data to Acquia Commerce Manager.
     *
     * @param Observer $observer Incoming Observer
     *
     * @return void
     */
    public function execute(Observer $observer)
    {
        $category = $observer->getEvent()->getCategory();
        $productIds = $category->getAffectedProductIds();

        $this->logger->info('Category saved.', [
            'observer' => $this->getLogName(),
            'category_id' => $category->getId(),
            'category_name' => $category->getName(),
            'products_updated' => $productIds,
        ]);


        $this->sendStoreCategories($category);
    }

    /**
     * {@inheritDoc}
     */
    protected function getLogName()
    {
        return ('CategorySaveObserver');
    }
}
