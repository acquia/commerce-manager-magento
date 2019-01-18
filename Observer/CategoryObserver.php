<?php

/**
 * Observer/CategoryObserver.php
 *
 * Acquia Commerce Connector Category Data Observer Abstract
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Observer;

use Acquia\CommerceManager\Model\Category\StoreTreeFactory as TreeFactory;
use Magento\Catalog\Api\CategoryRepositoryInterface;
use Magento\Catalog\Api\Data\CategoryInterface;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Store\Model\ScopeInterface;
use Acquia\CommerceManager\Helper\Acm as AcmHelper;
use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Magento\Framework\Webapi\ServiceOutputProcessor;
use Magento\Store\Model\StoreManager;
use Magento\Framework\Message\ManagerInterface as MessageManager;
use Psr\Log\LoggerInterface;

/**
 * CategoryObserver
 *
 * Acquia Commerce Connector Category Data Observer Abstract
 */
abstract class CategoryObserver extends ConnectorObserver
{
    /**
     * Connector Category Move Endpoint
     * @const ENDPOINT_CATEGORY_MOVE
     */
    const ENDPOINT_CATEGORY_MOVE = 'ingest/category/move';

    /**
     * Conductor filter_root_category config path.
     * @const FILTER_ROOT_CATEGORY_CONFIG_PATH
     */
    const FILTER_ROOT_CATEGORY_CONFIG_PATH = 'webapi/acquia_commerce_settings/filter_root_category';

    /**
     * Magento WebAPI Service Class Name (for output formatting)
     * @var string $serviceClassName
     */
    protected $serviceClassName = \Acquia\CommerceManager\Api\CategoryManagementInterface::class;

    /**
     * Magento WebAPI Service Method Name (for output formatting)
     * @var string $serviceMethodName
     */
    protected $serviceMethodName = 'getExtendedTree';

    /**
     * Magento Category Tree Instance Factory
     * @var TreeFactory $categoryTreeFactory
     */
    protected $categoryTreeFactory;

    /**
     * Magento Product Category Repository
     * @var CategoryRepositoryInterface $categoryRepository
     */
    protected $categoryRepository;

    /**
     * Scope Config factory.
     * @var \Magento\Framework\App\Config\ScopeConfigInterface
     */
    protected $scopeConfig;

    /**
     * Magento Store Manager
     * @var StoreManager $storeManager
     */
    private $storeManager;

  /**
     * Message manager.
     * @var MessageManager $messageManager
     */
    private $messageManager;

    /**
     * Constructor
     *
     * @param StoreManager $storeManager
     * @param TreeFactory $treeFactory
     * @param CategoryRepositoryInterface $categoryRepository
     * @param AcmHelper $acmHelper
     * @param ClientHelper $helper
     * @param ServiceOutputProcessor $outputProcessor
     * @param LoggerInterface $logger
     * @param ScopeConfigInterface $scopeConfig
     * @param MessageManager $messageManager
     */
    public function __construct(
        StoreManager $storeManager,
        TreeFactory $treeFactory,
        CategoryRepositoryInterface $categoryRepository,
        AcmHelper $acmHelper,
        ClientHelper $helper,
        ServiceOutputProcessor $outputProcessor,
        LoggerInterface $logger,
        ScopeConfigInterface $scopeConfig,
        MessageManager $messageManager
    ) {
        $this->storeManager = $storeManager;
        $this->categoryTreeFactory = $treeFactory;
        $this->categoryRepository = $categoryRepository;
        $this->scopeConfig = $scopeConfig;
        $this->messageManager = $messageManager;
        parent::__construct(
            $acmHelper,
            $helper,
            $outputProcessor,
            $logger
        );
    }

    /**
     * getLogName
     *
     * Get the name of the current observer for logging.
     *
     * @return string $name
     */
    abstract protected function getLogName();

    /**
     * sendStoreCategories
     *
     * Send a category to the Connector ingest endpoint, and if the category
     * is the default / base store, send all available store specific
     * categories as well.
     *
     * @param CategoryInterface $category Category to send.
     *
     * @return void
     */
    protected function sendStoreCategories(CategoryInterface $category)
    {
        $categoryStoreId = (int)($category->getStoreId()) ?: 0;

        // If the category data being saved is the base / default values,
        // send updated store specific categories as well (that may inherit
        // base field value updates) for all of the stores that the
        // category is assigned.

        if ($categoryStoreId === 0) {
            $stores = $category->getStoreIds();

            foreach ($stores as $storeId) {
                if ($storeId == 0) {
                    continue;
                }

                // Only send data for active stores
                /** @var \Magento\Store\Model\Store $storeModel */
                $storeModel = $this->storeManager->getStore($storeId);
                if (!$storeModel->isActive()) {
                    continue;
                }

                $this->logger->notice(
                    sprintf(
                        '%s: sending category for store %d.',
                        $this->getLogName(),
                        $storeId
                    ),
                    [
                        'name' => $category->getName(),
                        'id' => $category->getId()
                    ]
                );

                $storeCat = $this->categoryRepository->get(
                    $category->getId(),
                    $storeId
                );

                if ($storeCat) {
                    $this->sendCatTreeData($storeCat);
                }
            }
        }
        else {
            $this->sendCatTreeData($category);
        }

        $this->messageManager->addNotice(__('Your category update has been pushed to ACM for every impacted stores. These updates are going to be queued on ACM.'));
    }

    /**
     * sendCatTreeData
     *
     * Send product category tree data to the Connector API endpoint.
     *
     * @param CategoryInterface $category Category to send
     *
     * @return void
     */
    private function sendCatTreeData(CategoryInterface $category)
    {
        $storeId = $category->getStoreId();

        // Do not push category tree if root category is saved
        // and flag in config says filter root category.
        /** @var \Magento\Store\Model\Store $store */
        $store = $this->storeManager->getStore($storeId);
        if ($this->filterRootCategory($storeId) && $category->getId() === $store->getRootCategoryId()) {
            $this->logger->debug('Not pushing for save action on root category.');
            return;
        }

        $this->logger->debug('Pushing category tree data.', [
            'category_id' => $category->getId(),
            'store_id' => $storeId,
        ]);

        $tree = $this->categoryTreeFactory->create();
        $node = $tree->getRootNode($category);

        // Set the store id.
        $tree->setStoreId($storeId);
        $result = $tree->getTree($node);

        $edata = $this->formatApiOutput($result);
        $edata['store_id'] = $storeId;

        $doReq = function ($client, $opt) use ($edata) {
            $opt['json'] = $edata;
            return $client->post(self::ENDPOINT_CATEGORY_MOVE, $opt);
        };

        $this->tryRequest($doReq, $this->getLogName(), $storeId);
    }

    /**
     * Get filter root category config value.
     *
     * @param int $storeId
     *
     * @return mixed
     *   Config value.
     */
    private function filterRootCategory($storeId = 0)
    {
        return $this->scopeConfig->getValue(
            static::FILTER_ROOT_CATEGORY_CONFIG_PATH,
            ScopeInterface::SCOPE_STORE,
            $storeId
        );
    }
}
