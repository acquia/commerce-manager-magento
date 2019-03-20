<?php

/**
 * Acquia/CommerceManager/Helper/Data.php
 *
 * Acquia Commerce Connector API Helper
 *
 * All rights reserved. No unauthorized distribution or reproduction.
 */

namespace Acquia\CommerceManager\Helper;

use Acquia\CommerceManager\Model\ClientFactoryInterface;
use Acquia\Hmac\Guzzle\HmacAuthMiddleware;
use Acquia\Hmac\Key;
use GuzzleHttp\HandlerStack;
use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Framework\App\Helper\Context;
use Magento\Store\Model\ScopeInterface;
use GuzzleHttp\Client;
use GuzzleHttp\TransferStats;
use GuzzleHttp\Exception\RequestException;
use Magento\Framework\Message\ManagerInterface;
use Magento\Framework\Webapi\ServiceOutputProcessor;
use Magento\Store\Model\StoreManagerInterface;

/**
 * Data
 *
 * Acquia Commerce Connector API Helper
 */
class Data extends AbstractHelper
{
    /**
     * Guzzle API Client Factory
     * @var ClientFactoryInterface $clientFactory
     */
    private $clientFactory;

    /**
     * Magento UI Message Manager Interface
     * @var ManagerInterface $messageManager
     */
    private $messageManager;

    /**
     * @var \Magento\Store\Model\StoreManagerInterface
     */
    private $storeManager;

    /**
     * Constructor
     *
     * @param mixed Context $context
     * @param mixed ClientFactoryInterface $clientFactory
     * @param ManagerInterface $messageManager
     * @param StoreManagerInterface $storeManager
     */
    public function __construct(
        Context $context,
        ClientFactoryInterface $clientFactory,
        ManagerInterface $messageManager,
        StoreManagerInterface $storeManager
    )
    {
        $this->clientFactory = $clientFactory;
        $this->messageManager = $messageManager;
        $this->storeManager = $storeManager;
        parent::__construct($context);
    }

    /**
     * Get a Guzzle Client for Connector API operations.
     *
     * @param string $storeId Magento store ID.
     * @param string[] $options Override / extended options
     *
     * @return \GuzzleHttp\Client
     */
    public function getApiClient(string $storeId = null, array $options = [])
    {
        return $this->clientFactory->createClient(
            $this->getApiClientOptions($options,$storeId)
        );
    }

    /**
     * getApiClientOptions
     *
     * Get default API client connections options from config storage.
     *
     * @param string $storeId
     *   Magento store ID or NULL. NULL store ID causes Magento to revert to the default
     *   scope which should be the default store view of the default website.
     *   Must be store ID and not store code.
     * @param string[] $options
     *   Override / extended options
     *
     * @return string[] $options
     */
    public function getApiClientOptions(array $options = [],string $storeId = null)
    {
        $logger = $this->_logger;

        // Use store specific acm_id value, but if it's not set, then use store code.
        $acmUuid = $this->getApiConfigData('acquia_commerce/acm_id', $storeId);
        if (is_null($acmUuid)) {
            // A final desperate fallback. Possibly better to
            // throw an error if acmUuid is null
            $acmUuid = $this->storeManager->getStore($storeId)->getCode();
            // Log a warning at least
            $logger->warning("ACM UUID not set. Using ".$acmUuid." instead (Store ID ".$this->storeManager->getStore($storeId)->getId().").");
        }

        // Create key and middleware.
        $key = new Key(
            $this->getApiConfigData('acquia_commerce_security/ac_hmac_id', $storeId),
            base64_encode($this->getApiConfigData('acquia_commerce_security/ac_hmac_secret', $storeId))
        );
        $middleware = new HmacAuthMiddleware($key);
        // Register the middleware.
        $stack = HandlerStack::create();
        $stack->push($middleware);

        $basePath = $this->getApiConfigData('acquia_commerce/connector_url', $storeId);
        // Clean any trailing slashes or incumbent version paths.
        // Assumes any incumbent version path is "v0" to "v9"
        $basePath = preg_replace("#\\/v\\d\\/{0,1}$|\\/$#", "", $basePath);
        $apiVersionPath = $this->getApiConfigData('acquia_commerce/connector_api_version_path', $storeId);
        $baseUri = $basePath . "/" . $apiVersionPath . "/";

        return array_merge(
            [
                'base_uri' => $baseUri,
                'verify' => (bool)$this->getApiConfigData('acquia_commerce/connector_ssl_val'),
                'handler' => $stack,
                'headers' => [
                    'X-ACM-UUID' => $acmUuid,
                ],
            ],
            $options
        );
    }

    /**
     * getApiConfigData
     *
     * Retrieve information for Acquia Commerce API operations from config.
     *
     * @param string $field
     * @param int|null $storeId
     *
     * @return mixed
     */
    public function getApiConfigData($field, $storeId = null)
    {
        $path = 'webapi/' . $field;

        return $this->scopeConfig->getValue(
            $path,
            ScopeInterface::SCOPE_STORE,
            $storeId
        );
    }

    /**
     * tryRequest
     *
     * Make an API request to a Connector ingest endpoint.
     *
     * @param callable $doReq Guzzle request closure
     * @param string $storeId Magento store ID or null to use default store ID.
     * @param string $action Action Name (for logging)
     *
     * @return bool $success
     */
    public function tryRequest(callable $doReq, string $action, string $storeId = null)
    {
        if ($action == "") {
            throw new \RuntimeException('tryRequest: No logging action specified.');
        }

        $logger = $this->_logger;

        // This environment variable is intended to be set only on the
        // environments that are expected to push to ACM instances. It has been
        // introduced after having realised that cloned Magento databases for
        // use on local envs were unexpectedly pushing data to ACM.
        if (!getenv('ACQUIA_ACM_PUSH')) {
            $logger->error('Not sending anything to ACM as ACQUIA_ACM_PUSH environment variable is not set.');
            return (true);
        }

        // Check if the module is enabled.
        if ($this->getApiConfigData('acquia_commerce/connector_enable',$storeId) !== '1') {
            $logger->info("Connector isn't enabled for this store, no request has been made to middleware");
            return (true);
        }

        // Log transfer final endpoint and total time in debug mode.
        $reqOpts['on_stats'] =
            function (TransferStats $stats) use ($logger, $action) {
                $code =
                    ($stats->hasResponse()) ?
                        $stats->getResponse()->getStatusCode() :
                        0;

                $logger->info(sprintf(
                    '%s: Dispatched in %.4f to %s [%d]',
                    $action,
                    $stats->getTransferTime(),
                    $stats->getEffectiveUri(),
                    $code
                ));
            };

        try {
            $result = $doReq($this->getApiClient($storeId), $reqOpts);
        } catch (RequestException $e) {
            $mesg = sprintf(
                '%s: Exception dispatching: (%d) - %s',
                $action,
                $e->getCode(),
                $e->getMessage()
            );

            $this->_logger->error($mesg);
            $this->messageManager->addErrorMessage(sprintf(
                'Acquia Commerce sync action %s has failed with code %d,
                 troubleshooting may be required (see error logs for more details).',
                $action,
                $e->getCode()
            ));

            return (true);
        }

        return (true);
    }
}
