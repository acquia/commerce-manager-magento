<?php

namespace Acquia\CommerceManager;

use Magento\Framework\Component\ComponentRegistrar;
use Magento\Framework\Module\ModuleList;
use Magento\TestFramework\ObjectManager;

class CommerceManagerTest extends \PHPUnit\Framework\TestCase
{
    private $moduleName = 'Acquia_CommerceManager';

    /**
     * @var ObjectManager
     */
    private $objectManager;

    protected function setUp()
    {
        $this->objectManager = ObjectManager::getInstance();
    }

    public function testTheModuleIsRegistered()
    {
        $registrar = new ComponentRegistrar();
        $paths = $registrar->getPaths(ComponentRegistrar::MODULE);
        $this->assertArrayHasKey($this->moduleName, $paths);
    }

    public function testTheModuleIsKnownAndEnabledInTheRealEnvironment()
    {
        $directoryList = $this->objectManager->create(DirectoryList::class, ['root' => BP]);
        $configReader = $this->objectManager->create(DeploymentConfigReader::class, ['dirList' => $directoryList]);
        $deploymentConfig = $this->objectManager->create(DeploymentConfig::class, ['reader' => $configReader]);

        /** @var ModuleList $moduleList */
        $moduleList = $this->objectManager->create(ModuleList::class, ['config' => $deploymentConfig]);
        $message = sprintf('The module "%s" is not enabled in the real environment', $this->moduleName);
        $this->assertTrue($moduleList->has($this->moduleName), $message);
    }
}