<?php

/**
 * Acquia/CommerceManager/Helper/Stock.php
 *
 * Acquia Commerce Stock Helper
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Helper;

use Acquia\CommerceManager\Helper\Data as ClientHelper;
use Magento\CatalogInventory\Api\Data\StockItemInterfaceFactory;
use Magento\CatalogInventory\Api\StockItemCriteriaInterfaceFactory;
use Magento\CatalogInventory\Api\StockItemRepositoryInterface;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Framework\App\Helper\Context;
use Magento\Framework\Webapi\ServiceOutputProcessor;

/**
 * Stock
 *
 * Acquia Commerce Stock Helper
 */
class Stock extends AbstractHelper
{

    /**
     * Conductor Stock Update Endpoint
     *
     * @const ENDPOINT_PRODUCT_UPDATE
     */
    const ENDPOINT_STOCK_UPDATE = 'ingest/product-stock';

    /**
     * Magento WebAPI Output Processor
     *
     * @var ServiceOutputProcessor $serviceOutputProcessor
     */
    protected $serviceOutputProcessor;

    /**
     * Stock Item factory object.
     *
     * @var \Magento\CatalogInventory\Api\Data\StockItemInterfaceFactory
     */
    protected $stockItemFactory;

    /**
     * Stock Item Repository object.
     *
     * @var \Magento\CatalogInventory\Api\StockItemRepositoryInterface
     */
    protected $stockItemRepository;

    /**
     * Stock Item Criteria builder object.
     *
     * @var \Magento\CatalogInventory\Api\StockItemCriteriaInterfaceFactory
     */
    protected $stockItemCriteriaFactory;

    /**
     * Acquia Commerce Manager Client Helper
     *
     * @var ClientHelper $clientHelper
     */
    private $clientHelper;

    /**
     * Magento WebAPI Service Class Name (for output formatting of stock)
     *
     * @var string $stockServiceClassName
     */
    protected $serviceClassName = \Magento\CatalogInventory\Api\StockItemRepositoryInterface::class;

    /**
     * Magento WebAPI Service Method Name (for output formatting)
     *
     * @var string $serviceMethodName
     */
    protected $serviceMethodName = 'get';

    /**
     * Stock constructor.
     *
     * @param Context $context
     * @param ServiceOutputProcessor $outputProc
     * @param StockItemInterfaceFactory $stockItemFactory
     * @param StockItemCriteriaInterfaceFactory $stockItemCriteriaFactory
     * @param StockItemRepositoryInterface $stockItemRepository
     * @param Data $clientHelper
     */
    public function __construct(
        Context $context,
        ServiceOutputProcessor $outputProc,
        StockItemInterfaceFactory $stockItemFactory,
        StockItemCriteriaInterfaceFactory $stockItemCriteriaFactory,
        StockItemRepositoryInterface $stockItemRepository,
        ClientHelper $clientHelper
    ) {
        $this->serviceOutputProcessor = $outputProc;
        $this->stockItemFactory = $stockItemFactory;
        $this->stockItemCriteriaFactory = $stockItemCriteriaFactory;
        $this->stockItemRepository = $stockItemRepository;
        $this->clientHelper = $clientHelper;

        parent::__construct($context);
    }

    /**
     * pushStock.
     *
     * Helper function to push stock data through API.
     *
     * @param array $stockData Stock data.
     * @param string $storeId Magento store ID or null to use default store ID.
     */
    public function pushStock($stockData, $storeId = NULL) {
        // Send Connector request.
        $doReq = function ($client, $opt) use ($stockData) {
            $opt['json'] = $stockData;
            return $client->post(self::ENDPOINT_STOCK_UPDATE, $opt);
        };

        $this->clientHelper->tryRequest($doReq, 'pushStock', $storeId);
    }

    /**
     * getStockInfo.
     *
     * Get stock info for a product.
     *
     * @param $productId
     * @param $scopeId
     * @param $returnObject
     *
     * @return array
     */
    public function getStockInfo($productId, $scopeId = NULL, $returnObject = FALSE)
    {
        // When consumers are running, StockRegistry uses static cache.
        // With this cache applied, stock for a particular product if
        // changed multiple times within lifespan of consumer, it pushes
        // only the first change every-time.
        // To avoid the issue, we use the code used to cache stock info
        // directly. Code taken from below class::method:
        // Magento\CatalogInventory\Model\StockRegistryProvider::getStockItem().
        $criteria = $this->stockItemCriteriaFactory->create();
        $criteria->setProductsFilter($productId);

        if ($scopeId) {
            $criteria->setScopeFilter($scopeId);
        }

        $collection = $this->stockItemRepository->getList($criteria);
        $stockItem = current($collection->getItems());

        if (!($stockItem && $stockItem->getItemId())) {
            $stockItem = $this->stockItemFactory->create();
        }

        if ($returnObject) {
            return $stockItem;
        }

        $stock = $this->serviceOutputProcessor->process(
            $stockItem,
            $this->serviceClassName,
            $this->serviceMethodName
        );

        return $stock;
    }

    /**
     * Get stock mode (pull / push).
     *
     * @return bool
     *   TRUE if stock changes are to be pushed.
     */
    public function isStockPushEnabled() {
        $path = 'webapi/acquia_commerce_settings/push_stock';

        return (bool) $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );
    }

    /**
     * Get stock push batch size.
     *
     * @return int
     *   Batch size.
     */
    public function stockPushBatchSize() {
        $path = 'webapi/acquia_commerce_settings/stock_push_batch_size';

        return (int) $this->scopeConfig->getValue(
            $path,
            ScopeConfigInterface::SCOPE_TYPE_DEFAULT
        );
    }

}
