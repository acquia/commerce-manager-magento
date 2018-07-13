<?php

namespace Acquia\CommerceManager\Plugin;

class OrderRepositoryPlugin
{
    /** @var \Magento\Framework\App\State */
    private $appState;
    /** @var \Magento\Store\Model\StoreManagerInterface */
    private $storeManager;
    /** @var \Magento\Framework\Api\FilterFactory */
    private $filterFactory;
    /** @var  \Magento\Framework\Api\Search\FilterGroupFactory */
    private $filterGroupFactory;
    /** @var \Magento\Customer\Model\Config\Share  */
    private $configShare;

    public function __construct(
        \Magento\Framework\App\State $appState,
        \Magento\Store\Model\StoreManagerInterface $storeManager,
        \Magento\Framework\Api\FilterFactory $filterFactory,
        \Magento\Framework\Api\Search\FilterGroupFactory $filterGroupFactory,
        \Magento\Customer\Model\Config\Share $configShare
    )
    {
        $this->appState = $appState;
        $this->storeManager = $storeManager;
        $this->filterFactory = $filterFactory;
        $this->filterGroupFactory = $filterGroupFactory;
        $this->configShare = $configShare;
    }

    /**
     * We want to improve the Magento function.
     * Here we make allowances for
     * a multiple website e-commerce set-up with customers-per-website.
     *
     * We add the store_ids of one website_id to the order search
     * so that orders are fetched from all the stores of the website
     * corresponding to the store_id of the API URL (the current store).
     *
     * If the incoming searchCriteria already includes a store_id field then we honour it.
     *
     * @param \Magento\Sales\Api\OrderRepositoryInterface $subject
     * @param \Magento\Framework\Api\SearchCriteriaInterface $searchCriteria
     * @return array
     */
    public function beforeGetList(
        \Magento\Sales\Api\OrderRepositoryInterface $subject,
        \Magento\Framework\Api\SearchCriteriaInterface $searchCriteria
    )
    {
        // Restrict this plugin to restAPI only
        $notRestApi = !($this->appState->getAreaCode() == \Magento\Framework\App\Area::AREA_WEBAPI_REST);
        if ($notRestApi) {
            return [$searchCriteria];
        }

        // Only act if customers are website scoped and this is not the admin store (store_id == 0)
        if($this->configShare->isWebsiteScope() && $this->storeManager->getStore()->getId() != 0)
        {
            // Sales orders have store_id not website_id, so to return
            // all sales orders within the website we simply
            // seek all store IDs
            $storeIds = $this->storeManager->getStore()->getWebsite()->getStoreIds();

            // We cannot add to the existing searchCriteria's filters.
            // We must get them all, add our store_id filter,
            // then set all filter groups again
            /** @var array of \Magento\Framework\Api\Search\FilterGroup $filterGroup */
            $filterGroups = $searchCriteria->getFilterGroups();

            // But is the store ID already set?
            $alreadyHasStoreId = $this->filterHasStoreId($filterGroups);

            if(!$alreadyHasStoreId) {
                // customer_id can be set in a filter which
                // over-constrains the search when store_id is included.
                // However 'all store_ids' and customer_id should (must) all be consistent.
                // If they are not then you have an error in your search criteria.
                // We are not checking for consistency here. You are responsible for it.

                // You may know: filters within a filter group are combined 'OR'
                //               each filter group is combined 'AND'
                // Here we are adding (AND store_id IN [all store ids of this store's website])
                // so it gets its own filter group
                /** @var \Magento\Framework\Api\Filter $filter */
                $filter = $this->filterFactory->create();
                $filter->setField('store_id')
                       ->setConditionType('in')
                       ->setValue($storeIds);
                $filters = [$filter];

                /** @var \Magento\Framework\Api\Search\FilterGroup $filterGroup */
                $filterGroup = $this->filterGroupFactory->create();
                $filterGroup->setFilters($filters);
                $filterGroups[] = $filterGroup;

                $searchCriteria->setFilterGroups($filterGroups);
            }
        }
        return [$searchCriteria];
    }

    /**
     * Returns true if one of the filters is filtering on store_id.
     * Otherwise returns false.
     *
     * @param \Magento\Framework\Api\Search\FilterGroup[] $filterGroups
     * @return bool
     */
    protected function filterHasStoreId($filterGroups)
    {
        /** @var \Magento\Framework\Api\Search\FilterGroup $filterGroup */
        foreach($filterGroups as $filterGroup)
        {
            $filters = $filterGroup->getFilters();
            /** @var \Magento\Framework\Api\Filter $filter */
            foreach($filters as $filter)
            {
                if($filter->getField() == "store_id")
                {
                    return true;
                }
            }
        }
        return false;
    }

}
