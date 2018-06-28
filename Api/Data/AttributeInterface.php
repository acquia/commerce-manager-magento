<?php

/**
 * Acquia/CommerceManager/Api/Data/AttributeInterface.php
 *
 * Acquia Commerce Manager Attributes with Swatches data.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api\Data;

/**
 * Interface AttributeInterface
 * @api
 */
interface AttributeInterface extends \Magento\Eav\Api\Data\AttributeInterface
{
    /**
     * Set Swatches data.
     *
     * @param array $swatches
     * @return $this
     */
    public function setSwatches(array $swatches = []);

    /**
     * Get Swatches data.
     *
     * @return array
     */
    public function getSwatches();
}
