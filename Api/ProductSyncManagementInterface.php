<?php

/**
 * Acquia/CommerceManager/Api/ProductSyncManagementInterface.php
 *
 * Acquia Commerce Manager Product Syncronization Operations
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api;

/**
 * ProductSyncManagementInterface
 *
 * Acquia Commerce Manager Product Syncronization Operations
 * @api
 */
interface ProductSyncManagementInterface
{
    /**
     * syncProducts
     *
     * Send product data to Connector for a full product data sync, split by a current page / per page count.
     *
     * @param int $page_count Current Page
     * @param int $page_size Products Per Page
     * @param string $skus
     * @param string $category_id
     * @param int $async
     *
     * @return mixed true in async mode and products array in sync mode
     */
    public function syncProducts($page_count, $page_size = 50, $skus = '', $category_id = '', $async = 1);

}
