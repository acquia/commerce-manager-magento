<?php

/**
 * Acquia/CommerceManager/Model/Category/StoreTree.php
 *
 * Category Tree Supporting Loading Multiple Store Specific Trees
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Category;

use Acquia\CommerceManager\Model\ResourceModel\Category\StoreTree as TreeResource;
use Acquia\CommerceManager\Api\Data\ExtendedCategoryTreeInterfaceFactory as ExtendedTreeFactory;
use Magento\Catalog\Api\Data\CategoryTreeInterfaceFactory;
use Magento\Catalog\Model\Category\Tree;
use Magento\Catalog\Model\ResourceModel\Category\Collection;
use Magento\Catalog\Model\ResourceModel\Category\Collection\Factory;
use Magento\Store\Model\StoreManagerInterface;

/**
 * StoreTree
 *
 * Category Tree Supporting Loading Multiple Store Specific Trees
 */
class StoreTree extends Tree
{
    /**
     * Magento Category Collection Factory
     * @var Factory $categoryCollectionFactory
     */
    protected $categoryCollectionFactory;

    /**
     * Extended Category Tree Interface Factory
     * @var ExtendedTreeFactory $extTreeFactory
     */
    protected $extTreeFactory;

    /**
     * Store id.
     * @var int|string|null $storeId
     */
    protected $storeId = NULL;

    /**
     * Constructor
     *
     * @param TreeResource $categoryTree
     * @param StoreManagerInterface $storeManager
     * @param Collection $categoryCollection
     * @param CategoryTreeInterfaceFactory $treeFactory
     * @param Factory $categoryCollectionFactory
     * @param ExtendedTreeFactory $extTreeFactory
     */
    public function __construct(
        TreeResource $categoryTree,
        StoreManagerInterface $storeManager,
        Collection $categoryCollection,
        CategoryTreeInterfaceFactory $treeFactory,
        Factory $categoryCollectionFactory,
        ExtendedTreeFactory $extTreeFactory
    ) {
        $this->categoryCollectionFactory = $categoryCollectionFactory;
        $this->extTreeFactory = $extTreeFactory;

        return (parent::__construct(
            $categoryTree,
            $storeManager,
            $categoryCollection,
            $treeFactory
        ));
    }

    /**
     * {@inheritDoc}
     */
    public function getRootNode($category = null)
    {
        // Reset the category collection for new store data.
        $this->categoryCollection = $this->categoryCollectionFactory->create();

        return (parent::getRootNode($category));
    }

    /**
     * {@inheritDoc}
     */
    public function getTree($node, $depth = null, $currentLevel = 0)
    {
        /** @var \Acquia\CommerceManager\Api\Data\ExtendedCategoryTreeInterface[] $children */
        $children = $this->getChildren($node, $depth, $currentLevel);

        $store_id = $this->storeId ?: $node->getStoreId();

        /** @var \Acquia\CommerceManager\Api\Data\ExtendedCategoryTreeInterface $tree */
        $tree = $this->extTreeFactory->create();
        $tree->setId($node->getId())
            ->setParentId($node->getParentId())
            ->setName($node->getName())
            ->setPosition($node->getPosition())
            ->setLevel($node->getLevel())
            ->setIsActive($node->getIsActive())
            ->setProductCount($node->getProductCount())
            ->setStoreId($store_id)
            ->setDescription($node->getDescription())
            ->setIncludeInMenu($node->getIncludeInMenu())
            ->setChildrenData($children);

        return $tree;
    }

    /**
     * Add attributes to the loaded category collection, and allow
     * plugin methods to add additional extension attributes.
     *
     * @param Collection $collection
     *
     * @return Collection
     */
    public function prepareCollectionAttributes(Collection $collection)
    {
        $collection
            ->addAttributeToSelect('name')
            ->addAttributeToSelect('is_active')
            ->addAttributeToSelect('description')
            ->addAttributeToSelect('include_in_menu');

        return $collection;
    }

    /**
     * {@inheritDoc}
     */
    protected function getChildren($node, $depth, $currentLevel)
    {
        $children = [];

        if ($node->hasChildren()) {
            foreach ($node->getChildren() as $child) {
                if (($depth !== null) && ($depth <= $currentLevel)) {
                    break;
                }

                $children[] = $this->getTree($child, $depth, $currentLevel + 1);
            }
        }

        return ($children);
    }

    /**
     * {@inheritDoc}
     */
    protected function getNode(\Magento\Catalog\Model\Category $category)
    {
        $storeId = ($category->getStoreId() ?: 0);

        // Reset the category tree instance for new store data.
        $this->categoryTree->setStoreId($storeId);
        $this->categoryTree->getCollection()
            ->setProductStoreId($storeId)
            ->setStoreId($storeId);
        $this->categoryTree->setForceReload();

        $nodeId = $category->getId();
        $node = $this->categoryTree->loadNode($nodeId);
        $node->loadChildren();

        $this->prepareCollection($storeId);
        $this->categoryTree->addCollectionData($this->categoryCollection);

        return ($node);
    }

    /**
     * {@inheritDoc}
     */
    protected function prepareCollection($storeId = null)
    {
        // Pull current store id if none is passed from category.
        $storeId = ($storeId) ?: $this->storeManager->getStore()->getId();

        $this->categoryCollection
            ->setLoadProductCount(true)
            ->setProductStoreId($storeId)
            ->setStoreId($storeId);

        $this->prepareCollectionAttributes($this->categoryCollection);
    }

    /**
     * Set store id
     *
     * @param int|string $storeId
     * @return $this
     */
    public function setStoreId($storeId)
    {
      $this->storeId = $storeId;
      return $this;
    }
}
