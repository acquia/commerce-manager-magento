<?php

/**
 * Acquia/CommerceManager/Api/AcquiaProductAttributeRepositoryInterface.php
 *
 * Acquia Commerce Manager Product Attributes Repository.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Api;

/**
 * Interface RepositoryInterface must be implemented in new model
 * @api
 */
interface ProductAttributeRepositoryInterface extends \Magento\Framework\Api\MetadataServiceInterface
{
    /**
     * Retrieve all attributes for entity type
     *
     * @param \Magento\Framework\Api\SearchCriteriaInterface $searchCriteria
     * @return \Acquia\CommerceManager\Api\Data\AttributeInterface[]
     */
    public function getList(\Magento\Framework\Api\SearchCriteriaInterface $searchCriteria);

    /**
     * Retrieve specific attribute
     *
     * @param string $attributeCode
     * @return \Acquia\CommerceManager\Api\Data\AttributeInterface
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    public function get($attributeCode);
}
