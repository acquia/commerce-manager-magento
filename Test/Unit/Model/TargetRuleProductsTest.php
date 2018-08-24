<?php

// @codingStandardsIgnoreFile

namespace Acquia\CommerceManager\Test\Unit\Model;

use Magento\Catalog\Api\Data\ProductExtensionFactory;
use Magento\Catalog\Api\Data\ProductExtensionInterface;
use Magento\Catalog\Model\Product;
use Magento\Framework\Api\Data\ImageContentInterface;
use Magento\Framework\Api\ExtensibleDataInterface;
use Magento\Framework\Api\ExtensionAttributesFactory;
use Magento\Framework\TestFramework\Unit\Helper\ObjectManager as ObjectManagerHelper;
use Magento\Catalog\Model\Product\Attribute\Source\Status as Status;

/**
* TargetRuleProducts Test
*
* @SuppressWarnings(PHPMD.CouplingBetweenObjects)
* @SuppressWarnings(PHPMD.TooManyFields)
* @SuppressWarnings(PHPMD.ExcessivePublicCount)
*
*/
class TargetRuleProductsTest extends \PHPUnit\Framework\TestCase
{

    /**
     * @var ObjectManagerHelper
     */
    protected $objectManagerHelper;

    /**
     * @var \Acquia\CommerceManager\Model\TargetRuleProducts
     */
    protected $model;

    /**
     * @var \Magento\Framework\Registry|\PHPUnit_Framework_MockObject_MockObject
     */
    private $registry;

    /**
     * @var \Magento\Framework\Event\ManagerInterface|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $eventManagerMock;

    /**
     * @var ProductExtensionInterface|\PHPUnit_Framework_MockObject_MockObject
     */
    private $extensionAttributes;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    private $extensionAttributesFactory;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $attributeValueFactory;

    /**
     * @var \Magento\Framework\App\State|\PHPUnit_Framework_MockObject_MockObject
     */
    private $appStateMock;

    /**
     * @SuppressWarnings(PHPMD.ExcessiveMethodLength)
     */
    protected function setUp()
    {

        // Generate the mocks and stubs for the constructor

        // CONTEXT MOCK
        $this->eventManagerMock = $this->createPartialMock(
            \Magento\Framework\Event\ManagerInterface::class,
            ['dispatch']
        );
        $actionValidatorMock = $this->createMock(
            \Magento\Framework\Model\ActionValidator\RemoveAction::class
        );
        $actionValidatorMock->expects($this->any())->method('isAllowed')->will($this->returnValue(true));
        $cacheInterfaceMock = $this->createMock(\Magento\Framework\App\CacheInterface::class);
        // And set the area code to backend html (do we need this?)
        $this->appStateMock = $this->createPartialMock(
            \Magento\Framework\App\State::class,
            ['getAreaCode', 'isAreaCodeEmulated']
        );
        $this->appStateMock->expects($this->any())
            ->method('getAreaCode')
            ->will($this->returnValue(\Magento\Backend\App\Area\FrontNameResolver::AREA_CODE));

        $contextMock = $this->createPartialMock(
            \Magento\Framework\Model\Context::class,
            ['getEventDispatcher', 'getCacheManager', 'getAppState', 'getActionValidator'], [], '', false
        );
        $contextMock->expects($this->any())->method('getAppState')->will($this->returnValue($this->appStateMock));
        $contextMock->expects($this->any())
            ->method('getEventDispatcher')
            ->will($this->returnValue($this->eventManagerMock));
        $contextMock->expects($this->any())
            ->method('getCacheManager')
            ->will($this->returnValue($cacheInterfaceMock));
        $contextMock->expects($this->any())
            ->method('getActionValidator')
            ->will($this->returnValue($actionValidatorMock));
        // END CONTEXT MOCK

        // REGISTRY MOCK
        $this->registry = $this->getMockBuilder(\Magento\Framework\Registry::class)
            ->disableOriginalConstructor()
            ->getMock();
        // END REGISTRY MOCK

        // EXTENSION ATTRIBUTES FACTORY MOCK
        $this->extensionAttributes = $this->getMockBuilder(
            \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface::class)
            // do we need to stub? mock should suffice, no?
            // No. possibly no code generation now.
            // But if we stub these, what are we really testing?
            ->setMethods(['setData','getData'])
            // Some kind of magic:
            ->getMockForAbstractClass();
        $this->extensionAttributesFactory = $this->getMockBuilder(ExtensionAttributesFactory::class)
            ->disableOriginalConstructor()
            ->getMock();
        $this->extensionAttributesFactory
            ->expects($this->any())
            ->method('create')
            ->willReturn($this->extensionAttributes);
        // END EXTENSION ATTRIBUTES FACTORY MOCK

        // ATTRIBUTE VALUE FACTORY
        $this->attributeValueFactory = $this->getMockBuilder(\Magento\Framework\Api\AttributeValueFactory::class)
            ->disableOriginalConstructor()->getMock();
        // END ATTRIBUTE VALUE FACTORY

        $data = [];

        // Generate the model under test
        $this->objectManagerHelper = new ObjectManagerHelper($this);
        /** @var \Acquia\CommerceManager\Model\TargetRuleProducts model */
        $this->model = $this->objectManagerHelper->getObject(
            \Acquia\CommerceManager\Model\TargetRuleProducts::class,
            [
                'context' => $contextMock,
                'registry' => $this->registry,
                'extensionFactory' => $this->extensionAttributesFactory,
                'customAttributeFactory' => $this->attributeValueFactory,
                'data' => $data
            ]
        );
    }

    public function testAlwaysPass()
    {
        $this->assertTrue(true);
    }

    public function OFFtestAlwaysFail()
    {
        $this->fail("I expected this test to fail");
    }

    /**
     * Philosophically, what are we testing here?
     *   a) PHP 7 ability to apply strict typing?
     *      (why would we test that?)
     *   b) Ancestors of the class under test's ability to set and get data?
     *      (the ancestors are tested elsewhere (core)
     *       so why would we test that here with pointless unit tests?)
     */
    public function testGetCrosssell()
    {
        $expect = ["sku1","sku2","sku3"];
        $this->model->setCrosssell($expect);
        $this->assertEquals($expect,$this->model->getCrosssell());
    }
    public function testGetUpsell()
    {
        $expect = ["sku1","sku2","sku3"];
        $this->model->setUpsell($expect);
        $this->assertEquals($expect,$this->model->getUpsell());
    }
    public function testGetRelated()
    {
        $expect = ["sku1","sku2","sku3"];
        $this->model->setRelated($expect);
        $this->assertEquals($expect,$this->model->getRelated());
    }
    public function testGetSku()
    {
        $expect = "sku0";
        $this->model->setSku($expect);
        $this->assertEquals($expect,$this->model->getSku());
    }
    public function testGetType()
    {
        $expect = "all";
        $this->model->setType($expect);
        $this->assertEquals($expect,$this->model->getType());
    }

    /**
     * Another pointless unit test.
     */
    public function testGetExtensionAttributes()
    {
        $expect = ["sku8","sku9","sku10"];
        $type = "down_sell";

        $this->extensionAttributes
            ->expects($this->any())
            ->method('getData')
            ->with($type)
            ->will($this->returnValue($expect));

        $this->extensionAttributes
            ->expects($this->any())
            ->method('setData')
            ->with($type)
            ->will($this->returnValue($this->extensionAttributes));

        // Get (initiate) the extension attributes.
        $extensionAttributes = $this->model->getExtensionAttributes();
        // Set with our expectation.
        $extensionAttributes->setData($type, $expect);
        $this->model->setExtensionAttributes($extensionAttributes);

        // Test that we can retrieve what we expect.
        $this->assertEquals($expect,
            $this->model->getExtensionAttributes()->getData($type));
    }

    /**
     * Another pointless unit test
     */
    public function testAfterGetRelatedProductsByType()
    {
        // How to reference protected members in the mode under test?
        $_eventObject = 'object';
        $_eventPrefix = 'acquia_commercemanager_target_rule_products';

        $this->eventManagerMock->expects($this->once())
            ->method('dispatch')
            ->with(
                $_eventPrefix.'_after_get_related_products_by_type',
                [$_eventObject => $this->model]
            );

        $this->model->afterGetRelatedProductsByType();
    }

}
