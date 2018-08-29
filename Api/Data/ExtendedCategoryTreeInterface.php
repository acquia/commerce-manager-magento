<?php

/**
 * Acquia/CommerceManager/Api/Data/ExtendedCategoryTreeInterface.php
 *
 * Acquia Commerce Manager Extended Category Tree API Data Object
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api\Data;

use Magento\Catalog\Api\Data\CategoryTreeInterface;

/**
 * ExtendedCategoryTreeInterface
 *
 * Acquia Commerce Manager Extended Category Tree API Data Object
 * @api
 */
interface ExtendedCategoryTreeInterface extends CategoryTreeInterface
{
    /**
     * getDescription
     *
     * @return string
     */
    public function getDescription();

    /**
     * getIncludeInMenu
     *
     * @return bool
     */
    public function getIncludeInMenu();

    /**
     * getStoreId
     *
     * @return int
     */
    public function getStoreId();

    /**
     * getChildrenData
     *
     * @return \Acquia\CommerceManager\Api\Data\ExtendedCategoryTreeInterface[]
     */
    public function getChildrenData();

    /**
     * Retrieve existing extension attributes
     *
     * @return \Magento\Catalog\Api\Data\CategoryExtensionInterface|array
     */
    public function getExtensionAttributes();
}
