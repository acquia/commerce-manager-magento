<?php

/**
 * Acquia/CommerceManager/Model/Indexer/ProductRuleIndexBuilder.php
 *
 * Acquia Commerce Manager Catalog Rule Index Builder.
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Model\Indexer;

use Magento\CatalogRule\Model\Indexer\IndexBuilder;
use Acquia\CommerceManager\Helper\ProductBatch as BatchHelper;
use Magento\CatalogRule\Model\ResourceModel\Rule\CollectionFactory as RuleCollectionFactory;
use Magento\Framework\Pricing\PriceCurrencyInterface;
use Magento\Framework\App\ResourceConnection;
use Magento\Store\Model\StoreManagerInterface;
use Psr\Log\LoggerInterface;
use Magento\Eav\Model\Config;
use Magento\Framework\Stdlib\DateTime as StdlibDateTime;
use Magento\Framework\Stdlib\DateTime\DateTime;
use Magento\Catalog\Model\ProductFactory;

/**
 * ProductRuleIndexBuilder
 *
 * Acquia Commerce Manager Catalog Rule Index Builder.
 */
class ProductRuleIndexBuilder extends IndexBuilder
{

    /**
     * Contains the items pushed in queue already.
     *
     * @var array
     */
    protected $pushedItems = [];

    /**
     * If reindexing by id.
     *
     * @var bool
     */
    protected $reindexbyId = FALSE;

    /**
     * Batch helper.
     * @var ProductBatch $batchHelper
     */
    protected $batchHelper;

    /**
     * {@inheritdoc}
     */
    public function __construct(
        RuleCollectionFactory $ruleCollectionFactory,
        PriceCurrencyInterface $priceCurrency,
        ResourceConnection $resource,
        StoreManagerInterface $storeManager,
        LoggerInterface $logger,
        Config $eavConfig,
        StdlibDateTime $dateFormat,
        DateTime $dateTime,
        ProductFactory $productFactory,
        BatchHelper $batchHelper,
        $batchCount = 1000
    )
    {
        parent::__construct(
            $ruleCollectionFactory, $priceCurrency, $resource, $storeManager, $logger, $eavConfig, $dateFormat, $dateTime, $productFactory, $batchCount
        );
        $this->batchHelper = $batchHelper;
    }

    /**
     * {@inheritdoc}
     */
    protected function saveRuleProductPrices($arrData)
    {
        // Execute parent processing first.
        $object = parent::saveRuleProductPrices($arrData);

        // If its re-indexed by id, it means we not doing full-reindexing and
        //thus no need to process.
        if ($this->reindexbyId) {
          return $object;
        }

        $productIds = [];

        try {
            // Prepare products to send to queue.
            foreach ($arrData as $key => $data) {
              $productIds[$data['product_id']] = $data['product_id'];
            }

            // Get items which are not pushed to queue yet.
            $productIds = array_diff($productIds, $this->pushedItems);
            $this->pushedItems += $productIds;

            // If empty, means items pushed already.
            if (empty($productIds)) {
                return $object;
            }

            // Get batch size from config.
            $batchSize = $this->batchHelper->getProductPushBatchSize();

            foreach (array_chunk($productIds, $batchSize, TRUE) as $chunk) {
                $batch = [];

                foreach ($chunk as $productId) {
                    $batch[$productId] = [
                        'product_id' => $productId,
                        'store_id' => null,
                    ];
                }

                if (!empty($batch)) {
                    // Push product ids in queue in batch.
                    $this->batchHelper->addBatchToQueue($batch);

                    $this->logger->info('Added products to queue for pushing in background.', [
                        'observer' => 'ProductRuleIndexBuilder::saveRuleProductPrices()',
                        'batch' => $batch,
                    ]);
                }
            }
        } catch (\Exception $e) {
            // In case of ACM exception, we just log and process continues.
            $this->logger->info('Exception occurred with message:' . $e->getMessage(), [
                'observer' => 'ProductRuleIndexBuilder::saveRuleProductPrices()',
                'batch' => $productIds,
            ]);
        }

        return $object;
    }

    /**
     * {@inheritdoc}
     */
    public function reindexById($id)
    {
        // Marking this as true so that we know we indexing individual items and
        // not full re-index.
        $this->reindexbyId = TRUE;
        parent::reindexById($id);
    }

}
