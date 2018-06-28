<?php

/**
 * Acquia/CommerceManager/Model/Product/Attribute/OptionManagement.php
 *
 * Product Attribute Options with Swatches data.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Entity;

use Acquia\CommerceManager\Api\Data\AttributeInterface;
use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Api\AttributeValueFactory;
use Magento\Framework\Stdlib\DateTime\DateTimeFormatterInterface;
use Magento\Framework\App\ObjectManager;

/**
 * EAV Entity attribute model
 *
 * @method \Magento\Eav\Model\Entity\Attribute setOption($value)
 * @method \Magento\Eav\Api\Data\AttributeExtensionInterface getExtensionAttributes()
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 */
class Attribute extends \Magento\Catalog\Model\ResourceModel\Eav\Attribute implements \Acquia\CommerceManager\Api\Data\AttributeInterface
{

    private $swatches = [];

    /**
     * {@inheritdoc}
     */
    public function setSwatches(array $swatches = [])
    {
        $this->swatches = $swatches;
    }

    /**
     * {@inheritdoc}
     */
    public function getSwatches()
    {
        return $this->swatches;
    }
}
