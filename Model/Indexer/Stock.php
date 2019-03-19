<?php

/**
 * Acquia/CommerceManager/Model/Indexer/Stock.php
 *
 * Acquia Commerce Manager Stock Index Builder
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Indexer;

class Stock implements \Magento\Framework\Indexer\ActionInterface, \Magento\Framework\Mview\ActionInterface
{
    /**
     * Resource instance.
     *
     * @var \Magento\Framework\App\ResourceConnection
     */
    private $resource;

    /**
     * Resource Connection.
     *
     * @var \Magento\Framework\DB\Adapter\AdapterInterface
     */
    private $connection;

    /**
     * Acquia Connector Stock Helper.
     *
     * @var \Acquia\CommerceManager\Helper\Stock
     */
    private $stockHelper;

    /**
     * Store Manager object.
     *
     * @var \Magento\Store\Model\StoreManagerInterface
     */
    private $storeManager;

    /**
     * Logger object.
     *
     * @var \Psr\Log\LoggerInterface
     */
    private $logger;

    public function __construct(
        \Magento\Framework\App\ResourceConnection $resource,
        \Acquia\CommerceManager\Helper\Stock $stockHelper,
        \Magento\Store\Model\StoreManagerInterface $storeManager,
        \Psr\Log\LoggerInterface $logger
    ) {
        $this->resource = $resource;
        $this->stockHelper = $stockHelper;
        $this->storeManager = $storeManager;
        $this->logger = $logger;
    }

    /**
     * {@inheritdoc}
     */
    public function execute($ids)
    {
        if (!$this->stockHelper->isStockPushEnabled()) {
            $this->logger->info('ACM stock indexer - skipping stock push as it is disabled in config.');
            return;
        }

        $ids = is_array($ids) ? $ids : [$ids];

        $this->logger->info('ACM stock indexer - Processing stock items.', $ids);
        $stockItems = $this->getStockItems($ids);

        $stockBatch = [];
        $ignoredStockItems = [];

        foreach ($stockItems as $stockItem)
        {
            // Make data types consistent. ACM does not likes in-consistencies.
            $stockItem['is_in_stock'] = (bool) $stockItem['is_in_stock'];
            $stockItem['qty'] = (float) $stockItem['qty'];
            $stockItem['product_id'] = (int) $stockItem['product_id'];
            $stockItem['stock_id'] = (int) $stockItem['stock_id'];

            // Get the store id for stock item.
            $stockItem['store_id'] = $this->storeManager
                ->getWebsite($stockItem['website_id'])
                ->getDefaultGroup()
                ->getDefaultStoreId();

            // Do not push for default store / zero store id.
            if (empty($stockItem['store_id'])) {
                $ignoredStockItems[] = $stockItem;
                continue;
            }

            $stockBatch[$stockItem['store_id']][] = $stockItem;
        }

        $stockPushBatchSize = $this->stockHelper->stockPushBatchSize();

        foreach ($stockBatch ?? [] as $storeId => $stocks) {
            foreach (array_chunk($stocks, $stockPushBatchSize) as $storeStockBatch) {
                $this->logger->info('Pushing stock to ACM.', [
                    'store_id' => $storeId,
                    'stocks' => json_encode($storeStockBatch),
                ]);

                $this->stockHelper->pushStock($storeStockBatch, $storeId);
            }
        }

        if ($ignoredStockItems) {
            $this->logger->info('Ignored stock push for items from default store.', [
                'stocks' => json_encode($ignoredStockItems),
            ]);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function executeFull()
    {
        // Do nothing.
        $this->logger->info('ACM stock indexer - executeFull: doing nothing on purpose.');
    }

    /**
     * {@inheritdoc}
     */
    public function executeList(array $ids)
    {
        $this->logger->debug('ACM stock indexer - executeList.');
        $this->execute($ids);
    }

    /**
     * Execute partial indexation by ID
     *
     * @param int $id
     *
     * @return void
     */
    public function executeRow($id)
    {
        $this->logger->debug('ACM stock indexer - executeRow.');
        $this->execute([$id]);
    }

    /**
     * Retrieve connection instance
     *
     * @return bool|\Magento\Framework\DB\Adapter\AdapterInterface
     */
    private function getConnection()
    {
        if ($this->connection === null) {
            $this->connection = $this->resource->getConnection();
        }
        return $this->connection;
    }

    private function getStockItems(array $ids)
    {
        $select = $this->getConnection()->select();
        $select->from('cataloginventory_stock_item', '*');
        $select->joinLeft('catalog_product_entity', 'catalog_product_entity.entity_id = cataloginventory_stock_item.product_id', ['sku']);
        $select->where('cataloginventory_stock_item.item_id IN(?)', $ids);
        $select->where('catalog_product_entity.sku IS NOT NULL');
        return $select->query();
    }

}
