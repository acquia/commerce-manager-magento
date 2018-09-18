<?php

/**
 * Acquia/CommerceManager/Model/TargetRuleRepository.php
 *
 * Acquia Commerce Manager Target / Related Product Repository
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model;

use Acquia\CommerceManager\Model\TargetRuleProductsFactory as TargetRuleProductsFactory;
use Acquia\CommerceManager\Api\TargetRuleRepositoryInterface;
use Magento\Catalog\Api\ProductRepositoryInterface;
use Magento\Catalog\Api\Data\ProductInterface;
use Magento\Catalog\Model\Product\Visibility;
use Magento\Catalog\Model\ResourceModel\Product\Collection;
use Magento\Catalog\Model\ResourceModel\Product\CollectionFactory;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\ObjectManagerInterface;
use Magento\Framework\Module\ModuleListInterface;

/**
 * TargetRuleRepository
 *
 * Acquia Commerce Manager Target / Related Product Repository
 */
class TargetRuleRepository implements TargetRuleRepositoryInterface
{

    /**
     * Magento Target Rule Module Name
     * @const TARGETRULE_MODULE
     */
    const TARGETRULE_MODULE = 'Magento_TargetRule';

    /**
     * String key for meta type all
     * @const ALL_TYPES
     */
    const ALL_TYPES = 'all';

    /**
     * Magento Module List Service
     * @var ModuleListInterface $moduleList
     */
    private $moduleList;

    /**
     * Product Collection Factory
     * @var CollectionFactory $productCollectionFactory
     */
    private $productCollectionFactory;

    /**
     * Catalog Product Repository
     * @var ProductRepositoryInterface $productRepository
     */
    private $productRepository;

    /**
     * Target Rule Module Data Helper
     * @var \Magento\TargetRule\Helper\Data $targetRuleData
     */
    private $targetRuleData;

    /**
     * Target Rule Index Model
     * @var \Magento\TargetRule\Model\Index $targetRuleIndex
     */
    private $targetRuleIndex;

    /**
     * @var \Magento\TargetRule\Model\Rule
     */
    private $targetRuleClass;

    /**
     * Product Visibility Model
     * @var Visibility $visibility
     */
    private $visibility;

    /**
     * @var TargetRuleProductsFactory
     */
    private $targetRuleProductsFactory;

    /**
     * @var $targetRuleProducts \Acquia\CommerceManager\Model\TargetRuleProducts
     */
    public $relatedProducts;

    /**
     * @var $objectManager \Magento\Framework\App\ObjectManager
     */
    private $objectManager;

    /**
     * Constructor
     *
     * @param ObjectManagerInterface $objectManager
     * @param TargetRuleProductsFactory $targetRuleProductsFactory
     * @param ModuleListInterface $moduleList
     * @param ProductRepositoryInterface $productRepository
     * @param CollectionFactory $productCollectionFactory
     * @param Visibility $visibility
     */
    public function __construct(
        // Every good boy knows using the ObjectManager is sub-optimal.
        // Remember it is just a fudge to access EE classes without
        // The need for a separate EE module
        // We inject it here so that we can mock it for testing.
        ObjectManagerInterface $objectManager,
        // As you were:
        TargetRuleProductsFactory $targetRuleProductsFactory,
        ModuleListInterface $moduleList,
        ProductRepositoryInterface $productRepository,
        CollectionFactory $productCollectionFactory,
        Visibility $visibility
    ) {
        $this->objectManager = $objectManager;
        $this->targetRuleProductsFactory = $targetRuleProductsFactory;
        $this->moduleList = $moduleList;
        $this->productRepository = $productRepository;
        $this->productCollectionFactory = $productCollectionFactory;
        $this->visibility = $visibility;
    }

    /**
     * getProductsByType
     *
     * Returns lists of product relations by type and product sku.
     *
     * Public method here allows others to plugin around this method *but*
     * please add your custom relation types under extension_attributes
     * if you want them to be processed by ACM Commerce Connector so the
     * returned data object might look like this:
     * {
     *     "crosssell": [
     *         "sku1",
     *         "sku2"
     *     ],
     *     "related": [
     *         "sku3",
     *         "sku4"
     *     ],
     *     "upsell": [],
     *     "extension_attributes": {
     *         "popular_together": [
     *             "sku5",
     *             "sku6"
     *         ],
     *         "similar_colors": [
     *             "sku3x",
     *             "sku4x"
     *         ]
     *     }
     * }
     *
     * {@inheritDoc}
     *
     * @param string $sku Product SKU
     * @param string $type Link Type: ['related', 'upsell', 'crosssell', 'extension_attributes', 'all']
     *
     * @return \Acquia\CommerceManager\Api\Data\TargetRuleProductsInterface $products
     */
    public function getRelatedProductsByType($sku, $type)
    {
        return $this->_getRelatedProductsByType($sku, $type);
    }

    /**
     * {@inheritDoc}
     *
     * @param string $sku Product SKU
     * @param string $type Link Type: ['related', 'upsell', 'crosssell', 'extension', 'all']
     *
     * @return \Acquia\CommerceManager\Api\Data\TargetRuleProductsInterface $products
     */
    protected function _getRelatedProductsByType($sku, $type)
    {
        $this->relatedProducts = $this->targetRuleProductsFactory->create();
        $this->relatedProducts->setSku($sku);
        $this->relatedProducts->setType($type);

        // Initialise with empty arrays (a convention of the ACM Commerce Connector)
        $this->relatedProducts->setCrosssell([]);
        $this->relatedProducts->setUpsell([]);
        $this->relatedProducts->setRelated([]);

        if ($this->targetModuleEnabled()) {
            $ruleClass = $this->getTargetRuleClass();

            $types = [
                $this->relatedProducts::CROSS_SELL => $ruleClass::CROSS_SELLS,
                $this->relatedProducts::UP_SELL => $ruleClass::UP_SELLS,
                $this->relatedProducts::RELATED => $ruleClass::RELATED_PRODUCTS,
            ];

            // Only fetch relation types if we recognize the type.
            if (isset($types[$type]) || ($type == self::ALL_TYPES)) {
                $getTypes = [$type];
                if ($type === self::ALL_TYPES) {
                    $getTypes = array_keys($types);
                }

                $product = $this->productRepository->get($sku);

                foreach ($getTypes as $typeKey) {
                    $products = $this->getRuleProducts($types[$typeKey], $product);
                    $typeProducts = [];
                    foreach ($products as $typeProduct) {
                        $typeProducts[] = $typeProduct->getSku();
                    }
                    switch ($typeKey) {
                        case $this->relatedProducts::CROSS_SELL:
                            $this->relatedProducts->setCrosssell($typeProducts);
                            break;
                        case $this->relatedProducts::UP_SELL:
                            $this->relatedProducts->setUpsell($typeProducts);
                            break;
                        case $this->relatedProducts::RELATED;
                            $this->relatedProducts->setRelated($typeProducts);
                            break;
                        default:
                    }
                    $this->relatedProducts[$typeKey] = $typeProducts;
                }
            }
        }

        // Seek extensions
        // But please note extensions *must*
        // plugin after $this->getRelatedProductsByType()
        // or use the event observer (below)
        $this->relatedProducts->getExtensionAttributes();

        // Seek event-driven extensions
        $this->relatedProducts->afterGetRelatedProductsByType();

        return $this->relatedProducts;
    }

    /**
     * {@inheritDoc}
     *
     * @return bool $enabled
     */
    public function getTargetRulesEnabled()
    {
        return ($this->targetModuleEnabled());
    }

    /**
     * getRuleProducts
     *
     * Get a collection of products matching the type / current product.
     *
     * @return Collection $products
     */
    private function getRuleProducts($type, ProductInterface $product)
    {
        $limit = $this->getTargetRuleData()->getMaxProductsListResult();

        $indexModel = $this->getTargetRuleIndex()
            ->setType($type)
            ->setLimit($limit)
            ->setProduct($product)
            ->setExcludeProductIds([$product->getId()]);

        $productIds = $indexModel->getProductIds();
        $productIds = (count($productIds)) ? $productIds : [0];

        $collection = $this->productCollectionFactory->create();
        $collection->addFieldToFilter('entity_id', ['in' => $productIds]);

        $collection
            ->setPageSize($limit)
            ->setFlag('do_not_use_category_id', true)
            ->setVisibility($this->visibility->getVisibleInCatalogIds());

        return ($collection);
    }

    /**
     * getTargetRuleClass
     *
     * Get target rule class from the ObjectManager to prevent
     * coupling to EE.
     *
     * @return \Magento\TargetRule\Model\Rule $ruleData
     */
    private function getTargetRuleClass()
    {
        if (!$this->targetRuleClass) {
            $this->targetRuleClass = $this->objectManager->get(
                \Magento\TargetRule\Model\Rule::class
            );
        }
        return ($this->targetRuleClass);
    }

    /**
     * getTargetRuleData
     *
     * Get target rule data helper from the ObjectManager to prevent
     * coupling to EE.
     *
     * @return \Magento\TargetRule\Helper\Data $ruleData
     */
    private function getTargetRuleData()
    {
        if (!$this->targetRuleData) {
            $this->targetRuleData = $this->objectManager->get(
                \Magento\TargetRule\Helper\Data::class
            );
        }
        return ($this->targetRuleData);
    }

    /**
     * getTargetRuleIndex
     *
     * Build a Target Rule Index model from the ObjectManager to prevent
     * coupling to EE.
     *
     * @return \Magento\TargetRule\Model\Index $index
     */
    public function getTargetRuleIndex()
    {
        if (!$this->targetRuleIndex) {
            $this->targetRuleIndex = $this->objectManager->get(
                \Magento\TargetRule\Model\IndexFactory::class
            )->create();
        }

        return ($this->targetRuleIndex);
    }

    /**
     * targetModuleEnabled
     *
     * Check if the Magento EE Target Rules Module is installed / enabled.
     *
     * @return bool $enabled
     */
    private function targetModuleEnabled()
    {
        return ($this->moduleList->has(self::TARGETRULE_MODULE));
    }
}
