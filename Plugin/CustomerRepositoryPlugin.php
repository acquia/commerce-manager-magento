<?php

namespace Acquia\CommerceManager\Plugin;

class CustomerRepositoryPlugin
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
     * We want to improve the Magento function. Currently this function is called
     * for both 'save' and 'update' but the core function does a bad job of
     * determining which it is and in the process makes a mess of the website_id
     *
     * The main purpose of this plugin is to avoid the need to pass in website_id
     * in the request body because store_id is sufficient.
     * Now we need only send in store_id => create customer
     * *or* store_id and customer_id => update customer
     *
     * You can omit store_id. In that case we use the current store to get the store_id
     * and thence to get the website_id -- so make sure you are calling this function
     * using the full REST API URL including the store code so that store_id is set here.
     *
     * Code for setting website_id can be found here for example
     * \Magento\Customer\Model\AccountManagement::createAccountWithPasswordHash()
     *
     * Background on the need for this plugin can be found here
     * https://github.com/magento/magento2/issues/5115
     * https://magento.stackexchange.com/questions/122097/cant-put-customer-in-rest-api-magento2-fails-to-complete
     *
     * @param \Magento\Customer\Api\CustomerRepositoryInterface $subject
     * @param \Magento\Customer\Api\Data\CustomerInterface $customer
     * @param null $passwordHash
     * @return array
     */
    public function beforeSave($subject, \Magento\Customer\Api\Data\CustomerInterface $customer, $passwordHash = null)
    {
        // Restrict this plugin to restAPI only
        $notRestApi = !($this->appState->getAreaCode() == \Magento\Framework\App\Area::AREA_WEBAPI_REST);
        if ($notRestApi) {
            return [$customer, $passwordHash];
        }

        // Kill any website_id passed here. Do not send website_id. Use only store_id.
        $customer->setWebsiteId(null);

        // If we are creating a new customer, don't do anything more
        if (!$customer->getId())
        {
            return [$customer, $passwordHash];
        }

        // If we are updating a customer. Set the website_id based on the store ID
        $storeId = $customer->getStoreId();
        if ($storeId === null)
        {
            $storeId = $this->storeManager->getStore()->getId();
            $customer->setStoreId($storeId);
        }

        $websiteId = $this->storeManager->getStore($storeId)->getWebsiteId();
        // Please note that here we over-write any erroneously-sent-in website_id
        $customer->setWebsiteId($websiteId);

        return [$customer, $passwordHash];
    }

    /**
     * We want to improve the Magento function.
     * Here we make allowances for
     * a multiple website e-commerce set-up with customers-per-website.
     *
     * We add the website_id to the customer search
     * so that customers are fetched from the website corresponding to the
     * store_id of the API URL, that is, the current store.
     *
     * If the incoming searchCriteria already includes a website_id then we honour it.
     *
     * @param \Magento\Customer\Api\CustomerRepositoryInterface $subject
     * @param \Magento\Framework\Api\SearchCriteriaInterface $searchCriteria
     * @return array
     */
    public function beforeGetList(
        \Magento\Customer\Api\CustomerRepositoryInterface $subject,
        \Magento\Framework\Api\SearchCriteriaInterface $searchCriteria
    )
    {
        // Restrict this plugin to restAPI only
        $notRestApi = !($this->appState->getAreaCode() == \Magento\Framework\App\Area::AREA_WEBAPI_REST);
        if ($notRestApi) {
            return [$searchCriteria];
        }

        if($this->configShare->isWebsiteScope())
        {
            // Then respect the website of the current store.
            $storeId = $this->storeManager->getStore()->getId();
            $websiteId = $this->storeManager->getStore($storeId)->getWebsiteId();

            // Unless the store_id is 0 (admin store) -- just bail with no changes
            if($storeId == 0)
            {
                return [$searchCriteria];
            }

            // We cannot add to the existing searchCriteria's filters.
            // We must get them all, add our website_id filter,
            // then set all filter groups again
            $filterGroups = $searchCriteria->getFilterGroups();

            // But is the website ID already set?
            $alreadyHasWebsiteId = $this->filterHasWebsiteId($filterGroups);

            if(!$alreadyHasWebsiteId) {
                // store_id can be set in a filter, or 'created_in', both of which
                // over-constrain the search when website_id is included.
                // However website_id, store_id and created_in should (must) all be consistent.
                // If they are not then you have an error in your search criteria.
                // We are not checking for consistency here. You are responsible for it.

                // You may know: filters within a filter group are combined 'OR'
                //               each filter group is combined 'AND'
                // Here we are adding AND website_id = $websiteId
                // so it gets its own filter group
                /** @var \Magento\Framework\Api\Filter $filter */
                $filter = $this->filterFactory->create();
                $filter->setField('website_id')
                       ->setConditionType('eq')
                       ->setValue($websiteId);
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
     * Returns true if one of the filters is filtering on website_id.
     * Otherwise returns false.
     *
     * @param \Magento\Framework\Api\Search\FilterGroup[] $filterGroups
     * @return bool
     */
    protected function filterHasWebsiteId($filterGroups)
    {
        /** @var \Magento\Framework\Api\Search\FilterGroup $filterGroup */
        foreach($filterGroups as $filterGroup)
        {
            $filters = $filterGroup->getFilters();
            /** @var \Magento\Framework\Api\Filter $filter */
            foreach($filters as $filter)
            {
                if($filter->getField() == "website_id")
                {
                    return true;
                }
            }
        }
        return false;
    }

}
