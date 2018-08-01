<?php

/**
 * Acquia/CommerceManager/Plugin/Stock.php
 *
 * Acquia Connector Plugin to override StockUpdateIdx.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Plugin;

use Magento\Catalog\Api\ProductRepositoryInterface;
use Acquia\CommerceManager\Helper\Stock as StockHelper;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\InventoryMessageBus\Model\ResourceModel\StockUpdateIdx;
use Psr\Log\LoggerInterface;

class Stock
{

    /**
     * Acquia Connector Stock Helper
     *
     * @var StockHelper
     */
    private $stockHelper;

    /**
     * Magento Product Repository
     *
     * @var ProductRepositoryInterface
     */
    private $productRepository;

    /**
     * Magento Database Resource
     *
     * @var ResourceConnection $resource
     */
    protected $resource;

    /**
     * Magento Database Connection
     *
     * @var AdapterInterface $connection
     */
    protected $connection;

    /**
     * Logger service.
     *
     * @var LoggerInterface
     */
    private $logger;

    /**
     * Product IDs array to be updated.
     *
     * @var array
     */
    private $product_ids = [];

    /**
     * Stock constructor.
     *
     * @param StockHelper $stockHelper
     * @param ProductRepositoryInterface $productRepository
     * @param ResourceConnection $resource
     * @param LoggerInterface $logger
     */
    public function __construct(
        StockHelper $stockHelper,
        ProductRepositoryInterface $productRepository,
        ResourceConnection $resource,
        LoggerInterface $logger
    ) {
        $this->stockHelper = $stockHelper;
        $this->productRepository = $productRepository;
        $this->resource = $resource;
        $this->connection = $resource->getConnection();
        $this->logger = $logger;
    }

    /**
     * beforeUpdateStockTable
     *
     * @param StockUpdateIdx $idx
     * @param array          $batch
     * @param array          $websiteIds
     *
     * @return array
     */
    public function beforeUpdateStockTable(StockUpdateIdx $idx, $batch,
        $websiteIds
    ) {
        // Don't do anything if we are not pushing stock changes.
        if ($this->stockHelper->getStockMode() !== 'push') {
            return [$batch, $websiteIds];
        }

        $cleanedBatch = $this->assignProperSkus($batch);

        foreach ($cleanedBatch as $row) {
            if (!empty($row['product_id'])) {
                $row['website_ids'] = $websiteIds;
                $this->product_ids[$row['product_id']] = $row;
            }
        }

        $this->filterProductsWithStockUpdate($websiteIds);

        return [$batch, $websiteIds];
    }

    /**
     * Push details about stock change to queue for pushing further.
     *
     * @param StockUpdateIdx $idx
     * @param mix $result
     *
     * @return mixed
     */
    public function afterUpdateStockTable(StockUpdateIdx $idx, $result)
    {
        if (!empty($this->product_ids)) {
            foreach ($this->product_ids as $product_id => $row) {
                $this->logger->info('afterUpdateStockTable: Adding product to queue.', [
                    'product_id' => $product_id,
                ]);

                $data = [
                    'id' => $product_id,
                    'sku' => $row['exact_sku'],
                    'website_ids' => $row['website_ids'],
                    'qty' => $row['qty'],
                ];

                $message = json_encode($data);

                $this->stockHelper->addStockMessageToQueue($message);

                unset($this->product_ids[$product_id]);
            }
        }

        return $result;
    }

    /**
     * Push all products to queue.
     *
     * This will be used for MODE:NONZERO and MODE:FULL.
     *
     * @param StockUpdateIdx $idx
     * @param callable $original
     * @param $stockId
     */
    public function aroundApplyIndexTableToStockTable(StockUpdateIdx $idx, callable $original, $stockId) {
        $this->logger->info('Inside afterApplyIndexTableToStockTable', ['stock_id' => $stockId]);

        // Execute the original method and remember the result.
        $original($stockId);

        // Load all products with stock.
        $select = $this->connection->select()->from(
            $this->resource->getTableName('cataloginventory_stock_item'),
            ['product_id', 'website_id', 'qty']
        );

        $select->where('stock_id IN (?)', $stockId);

        $products = $this->connection->fetchAll($select);

        if (empty($products)) {
            return;
        }

        foreach ($products as $row) {
            $data = [
                'id' => $row['product_id'],
                'website_ids' => $row['website_id'],
                'qty' => $row['qty'],
            ];

            $message = json_encode($data);

            $this->stockHelper->addStockMessageToQueue($message);
        }
    }

    /**
     * assignProperSkus.
     *
     * Assign proper SKUs to each row in batch. For configurable products we
     * usually get two entries for both parent and child products but with same
     * (child) SKUs in both. This blocks our flow as we rely heavily on SKUs.
     *
     * @param array $batch
     *
     * @return array
     */
    private function assignProperSkus($batch)
    {
        $productIds = array_column($batch, 'product_id');

        $select = $this->connection->select()->from(
            $this->resource->getTableName('catalog_product_entity'),
            ['entity_id', 'sku']
        );

        $select->where('entity_id IN (?)', $productIds);

        $records = $this->connection->fetchPairs($select);

        foreach ($batch as $key => $row) {
            if (isset($records[$row['product_id']])) {
                $batch[$key]['exact_sku'] = $records[$row['product_id']];
            } else {
                $batch[$key]['exact_sku'] = $row['sku'];
            }
        }

        return $batch;
    }

    /**
     * filterProductsWithStockUpdate.
     *
     * Keep only those product ids in $this->product_ids for which stock is
     * updated.
     *
     * @param array $websiteIds
     */
    protected function filterProductsWithStockUpdate($websiteIds)
    {
        if (empty($this->product_ids)) {
            return;
        }

        // Tables like cataloginventory_stock_status_idx / cataloginventory_stock_status
        // are not updated in real-time. To ensure we check if stock is changed
        // against latest information we use cataloginventory_stock_item
        // which is updated real-time.
        $select = $this->connection->select()->from(
            $this->resource->getTableName('cataloginventory_stock_item'),
            ['product_id', 'qty']
        );

        $select->where('product_id IN (?)', array_keys($this->product_ids));
        $select->where('website_id IN (?)', $websiteIds);

        $productQuantities = $this->connection->fetchPairs($select);

        foreach ($this->product_ids as $product_id => $data) {
            $new_quantity = $data['qty'];

            if (isset($productQuantities[$product_id]) && $productQuantities[$product_id] == $new_quantity) {
                // Stock not changed for this product, we won't trigger stock
                // push for this one.
                $this->logger->debug('beforeUpdateStockTable: Stock not changed.', [
                        'product_id' => $product_id,
                        'old_stock' => $productQuantities[$product_id],
                        'new_stock' => $new_quantity,
                        'website_ids' => json_encode($websiteIds),
                    ]
                );

                // Stock not updated or is the same for this product.
                unset($this->product_ids[$product_id]);
            }
            else {
                // We might not have any entry in DB for new products.
                $oldStock = $productQuantities[$product_id] ?? 'NA';

                // Stock changed for this product, we will trigger stock push
                // for this one.
                $this->logger->debug('beforeUpdateStockTable: Stock changed.', [
                    'product_id' => $product_id,
                    'old_stock' => $oldStock,
                    'new_stock' => $new_quantity,
                    'website_ids' => json_encode($websiteIds),
                ]);
            }
        }
    }

}
