<?php

/**
 * Acquia/CommerceManager/Model/Product/Attribute/Repository.php
 *
 * Product Attribute Options with Swatches data.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Product\Attribute;

class Repository implements \Acquia\CommerceManager\Api\ProductAttributeRepositoryInterface
{

    /**
     * @var \Magento\Catalog\Api\ProductAttributeRepositoryInterface
     */
    protected $productAttributeRepository;

    /**
     * @var \Magento\Eav\Model\AttributeRepository
     */
    protected $attributeRepository;

    /**
     * @var \Magento\Framework\Reflection\DataObjectProcessor
     */
    protected $dataProcessor;

    /**
     * @var \Magento\Framework\Api\SearchCriteriaBuilder
     */
    protected $searchCriteriaBuilder;

    /**
     * @var \Magento\Swatches\Helper\Data
     */
    protected $swatchHelper;

    /**
     * @var \Magento\Swatches\Helper\Media
     */
    protected $mediaHelper;

    /**
     * Repository constructor.
     *
     * @param \Magento\Catalog\Api\ProductAttributeRepositoryInterface $productAttributeRepository
     * @param \Magento\Eav\Model\AttributeRepository $attributeRepository
     * @param \Magento\Framework\Reflection\DataObjectProcessor $dataProcessor
     * @param \Magento\Framework\Api\SearchCriteriaBuilder $searchCriteriaBuilder
     * @param \Magento\Swatches\Helper\Data $swatchHelper
     * @param \Magento\Swatches\Helper\Media $mediaHelper
     */
    public function __construct(
        \Magento\Catalog\Api\ProductAttributeRepositoryInterface $productAttributeRepository,
        \Magento\Eav\Model\AttributeRepository $attributeRepository,
        \Magento\Framework\Reflection\DataObjectProcessor $dataProcessor,
        \Magento\Framework\Api\SearchCriteriaBuilder $searchCriteriaBuilder,
        \Magento\Swatches\Helper\Data $swatchHelper,
        \Magento\Swatches\Helper\Media $mediaHelper
    ) {
        $this->productAttributeRepository = $productAttributeRepository;
        $this->attributeRepository = $attributeRepository;
        $this->dataProcessor = $dataProcessor;
        $this->searchCriteriaBuilder = $searchCriteriaBuilder;
        $this->swatchHelper = $swatchHelper;
        $this->mediaHelper = $mediaHelper;
    }

    /**
     * {@inheritdoc}
     */
    public function get($attributeCode)
    {
        $item = $this->productAttributeRepository->get($attributeCode);
        $item->setSwatches($this->getSwatches($item->getAttributeCode(), $item->getOptions()));

        return $item;
    }

    /**
     * {@inheritdoc}
     */
    public function getList(\Magento\Framework\Api\SearchCriteriaInterface $searchCriteria)
    {
        $list = $this->productAttributeRepository->getList($searchCriteria);

        $items = $list->getItems();

        foreach ($items as &$item) {
            $item->setSwatches($this->getSwatches($item->getAttributeCode(), $item->getOptions()));
        }

        return $items;
    }

    /**
     * @param string $attribute_code
     * @param \Magento\Eav\Api\Data\AttributeOptionInterface[] $options
     * @return array
     */
    private function getSwatches($attribute_code, $options) {
        $attribute = $this->attributeRepository->get(\Magento\Catalog\Api\Data\ProductAttributeInterface::ENTITY_TYPE_CODE, $attribute_code);

        $response = [];
        $option_ids = [];

        foreach ($options as $index => $option) {
            // Strip empty option.
            if ($option->getValue()) {
                 $option_ids[$option->getValue()] = $index;
            }
        }

        if ($attribute->getData('additional_data')) {
            $isSwatch = $this->swatchHelper->isVisualSwatch($attribute) || $this->swatchHelper->isTextSwatch($attribute);

            if ($isSwatch) {
                $swatches = $this->swatchHelper->getSwatchesByOptionsId(array_keys($option_ids));

                foreach ($swatches as $option_id => $swatch) {
                    // Sanity check.
                    if (empty($option_ids[$option_id])) {
                        continue;
                    }

                    // Do we really want to pass incomplete data? I think no.
                    if (empty($swatch['value'])) {
                        continue;
                    }

                    $row = [];
                    $row['option_id'] = $option_id;
                    $row['swatch_type'] = $swatch['type'];

                    if ($swatch['type'] == \Magento\Swatches\Model\Swatch::SWATCH_TYPE_VISUAL_IMAGE) {
                        try {
                            $row['swatch'] = $this->mediaHelper->getSwatchAttributeImage(
                                \Magento\Swatches\Model\Swatch::SWATCH_IMAGE_NAME,
                                $swatch['value']
                            );
                        }
                        catch (\Exception $e) {
                            // Mostly on dev/local and other envs we get this issue.
                            // {file} does not exists.
                            continue;
                        }
                    }
                    else {
                        $row['swatch'] = $swatch['value'];
                    }

                    $response[$option_id] = $row;
                }
            }
        }

        return $response;
    }

    /**
     * {@inheritdoc}
     * @SuppressWarnings(PHPMD.UnusedFormalParameter)
     */
    public function getCustomAttributesMetadata($dataObjectClassName = null)
    {
        return $this->getList($this->searchCriteriaBuilder->create())->getItems();
    }

}
