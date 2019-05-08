<?php

namespace Acquia\CommerceManager\Setup;

use Magento\Framework\Indexer\IndexerRegistry;
use Magento\Framework\Setup\UpgradeDataInterface;
use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\ModuleDataSetupInterface;

/**
 * @codeCoverageIgnore
 */
class UpgradeData implements UpgradeDataInterface
{

    /**
     * @var \Psr\Log\LoggerInterface $logger
     */
    private $logger;

    /**
     * @var IndexerRegistry
     */
    private $indexerRegistry;

    /**
     * @var \Magento\Integration\Api\IntegrationServiceInterface
     */
    private $integrationService;

    /**
     * UpgradeData constructor.
     *
     * @param \Magento\Integration\Api\IntegrationServiceInterface $integrationService
     * @param IndexerRegistry $indexerRegistry
     * @param \Psr\Log\LoggerInterface $logger
     */
    public function __construct(
        \Magento\Integration\Api\IntegrationServiceInterface $integrationService,
        IndexerRegistry $indexerRegistry,
        \Psr\Log\LoggerInterface $logger //log injection
    ) {
        $this->integrationService = $integrationService;
        $this->indexerRegistry = $indexerRegistry;
        $this->logger = $logger;
    }

    /**
     * {@inheritdoc}
     * @SuppressWarnings(PHPMD.ExcessiveMethodLength)
     */
    public function upgrade(ModuleDataSetupInterface $setup, ModuleContextInterface $context)
    {
        $this->logger->info("UPGRADED ACM MODULE DATA FROM ".$context->getVersion());
        $setup->startSetup();

        if (version_compare($context->getVersion(), '1.0.1') < 0) {
            //code to upgrade to 1.0.1
            $this->logger->info("Upgraded to 1.0.1");
        }

        if (version_compare($context->getVersion(), '1.0.2') < 0) {
            //code to upgrade to 1.0.2
            $this->logger->info("Upgraded to 1.0.2");
        }

        if (version_compare($context->getVersion(), '1.1.1') < 0) {
            //code to upgrade to 1.1.1
            $this->logger->info("Upgraded to 1.1.1");
        }

        if (version_compare($context->getVersion(), '1.1.2') < 0) {
            //code to upgrade to 1.1.2

            //see if there is an incumbent Acquia Commerce Connector integration
            $deprecatedIntegration = $this->integrationService->findByName("AcquiaConductor");
            if ($deprecatedIntegration->getId()) {
                $oldData = $this->integrationService->delete($deprecatedIntegration->getId());
            }
            $this->logger->info("Upgraded to 1.1.2");
        }

        if (version_compare($context->getVersion(), '1.1.3') < 0) {
            $this->indexerRegistry->get('acq_salesrule_product')->reindexAll();
            $this->logger->info('ACM data upgraded to 1.1.3, Sales Rule indexer data re-indexed.');
        }

        if (version_compare($context->getVersion(), '1.1.4') < 0) {
            // Set indexer mode to on schedule.
            $this->indexerRegistry->get('acq_cataloginventory_stock')->setScheduled(true);

            $this->logger->info('ACM upgraded to 1.1.4, stock indexer added and set to index on schedule.');
        }

        $setup->endSetup();
    }
}
