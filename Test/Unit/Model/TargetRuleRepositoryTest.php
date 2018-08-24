<?php

// @codingStandardsIgnoreFile

namespace Acquia\CommerceManager\Test\Unit\Model;

use Magento\Framework\TestFramework\Unit\Helper\ObjectManager;
use Magento\Catalog\Model\ResourceModel\Product\Collection as ProductCollection;

/**
 * Class ProductRepositoryTest
 *
 * @package Magento\Catalog\Test\Unit\Model
 * @SuppressWarnings(PHPMD.TooManyFields)
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 * @SuppressWarnings(PHPMD.ExcessiveMethodLength)
 */
class TargetRuleRepositoryTest extends \PHPUnit\Framework\TestCase
{

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $targetRuleProductsMock;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $targetRuleProductsFactoryMock;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $moduleListMock;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $productRepositoryMock;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $productCollectionFactoryMock;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    protected $visibilityMock;

    /**
     * @var \Acquia\CommerceManager\Model\TargetRuleRepository
     */
    protected $model;

    /**
     * @var \Magento\Framework\TestFramework\Unit\Helper\ObjectManager
     */
    protected $objectManager;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject|\Magento\Framework\App\ObjectManager
     */
    protected $objectManagerMock;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject|\Magento\TargetRule\Model\Index
     */
    protected $targetRuleIndex;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject|\Magento\TargetRule\Model\IndexFactory
     */
    protected $contrivedTargetRuleIndexFactory;

    /**
     * @var ProductCollection|\PHPUnit_Framework_MockObject_MockObject
     */
    private $productCollectionMock;

    /**
     * @var int[]
     */
    private $arbitraryProductIds;

    /**
     * @SuppressWarnings(PHPMD.ExcessiveMethodLength)
     */
    protected function setUp()
    {
        // Bail now if the module Magento_TargetRule doesn't exist.
        // Try to instantiate the class (the test framework will return a mock
        // or throw an exception if the class does not exist).
        try
        {
            $targetRule = \Magento\Framework\App\ObjectManager::getInstance()->get(
                \Magento\TargetRule\Model\Rule::class
            );
        }
        catch (\Exception $e)
        {
            // This function stops the tests proceeding.
            $this->markTestSkipped("Enterprise Edition module Magento_TargetRule is not present");
        }

        $this->objectManager = new ObjectManager($this);

        $this->arbitraryProductIds = [14,15,16,17,18,19,20];


        // START Constructor mocks (noting partials should have type declarations of Mock|ClassName).

        // We use a private function to return the TargetRuleIndex via its factory using the object manager.
        // Using the object manager is an anti-pattern but a necessary evil for now
        // until we split this module into an EE dedicated module and a CE main module.
        // So:
        // Make the factory to make the mock to get thing we want returned.
        // a) The factory.
        $this->contrivedTargetRuleIndexFactory = $this->createPartialMock(
            \Magento\TargetRule\Model\IndexFactory::class,
            ['create']
        );
        // b) The mock we want.
        $this->targetRuleIndex = $this->createPartialMock(
            \Magento\TargetRule\Model\Index::class,
            ['getProductIds']
        );
        // c) The thing we want the mock to return.
        $this->targetRuleIndex->expects($this->any())
            ->method("getProductIds")
            ->withAnyParameters()
            ->willReturn(
                $this->arbitraryProductIds
            );
        // d) Make the contrived factory create the mock.
        $this->contrivedTargetRuleIndexFactory->expects($this->atMost(1))
            ->method("create")
            ->withAnyParameters()
            ->willReturn($this->targetRuleIndex);

        // e) Mock the object manager to return the contrived target rule index object.
        $this->objectManagerMock = $this->createPartialMock(
            \Magento\Framework\App\ObjectManager::class,
            ['get']
        );
        // Actually we only want to stub one of the gets so now we need to hoop jump.
        // So re-instate the get function for the classes we want
        // e.1) Create an array of arrays of arguments-to-return-values.
        $targetRuleModelRuleMock = \Magento\Framework\App\ObjectManager::getInstance()->get(\Magento\TargetRule\Model\Rule::class);
        $targetRuleHelperDataMock = \Magento\Framework\App\ObjectManager::getInstance()->get(\Magento\TargetRule\Helper\Data::class);
        $objectManagerMockMap = array(
            array(\Magento\TargetRule\Model\Rule::class, $targetRuleModelRuleMock),
            array(\Magento\TargetRule\Helper\Data::class, $targetRuleHelperDataMock),
            array(\Magento\TargetRule\Model\IndexFactory::class, $this->contrivedTargetRuleIndexFactory)
        );
        // e.2) Set the stubbed 'get' method to return values based on the array ie map.
        $this->objectManagerMock->expects($this->any())
            ->method("get")
            ->will($this->returnValueMap($objectManagerMockMap));

        // The TargetRuleRepository class is a repository of TargetRuleProducts objects
        // We choose to use real TargetRuleProducts objects so that the setters and getters work.
        // Create a *real* instance and ensure it is returned from its factory.
        $this->targetRuleProductsFactoryMock = $this->createPartialMock(
            \Acquia\CommerceManager\Model\TargetRuleProductsFactory::class,
            ['create']
        );
        $this->targetRuleProductsFactoryMock->expects($this->any())
            ->method("create")
            ->withAnyParameters()
            ->willReturn($this->getNewRealTargetRuleProductsObject());

        $this->moduleListMock = $this->createMock(\Magento\Framework\Module\ModuleListInterface::class);
        $this->productRepositoryMock = $this->createMock(\Magento\Catalog\Api\ProductRepositoryInterface::class);

        // Create a product collection mock and ensure it is returned from its factory mock
        // Note we don't use ProductCollection (Why? don't know. See Magento core tests for examples)
        // See also $this->objectManager->getCollectionMock(className, $dataArrayFotTheIterator);
        $this->productCollectionMock = $this->createMock(ProductCollection::class);

        $this->productCollectionFactoryMock = $this->createPartialMock(
            \Magento\Catalog\Model\ResourceModel\Product\CollectionFactory::class,
            ["create"]);
        $this->visibilityMock = $this->createMock(\Magento\Catalog\Model\Product\Visibility::class);
        // END Constructor mocks

        // the model under test ** UNIT TEST **
        $this->model = $this->objectManager->getObject(
            \Acquia\CommerceManager\Model\TargetRuleRepository::class,
            [
                'objectManager' => $this->objectManagerMock,
                'targetRuleProductsFactory' => $this->targetRuleProductsFactoryMock,
                'moduleList' => $this->moduleListMock,
                'productRepository' => $this->productRepositoryMock,
                'productCollectionFactory' => $this->productCollectionFactoryMock,
                'visibility' => $this->visibilityMock
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

    public function testGetTargetRulesEnabledTrue()
    {
        $this->moduleListMock->expects($this->once())
            ->method('has')
            ->with($this->model::TARGETRULE_MODULE)
            ->will($this->returnValue(true));
        $this->assertTrue($this->model->getTargetRulesEnabled());
    }

    public function testGetTargetRulesEnabledFalse()
    {
        $this->moduleListMock->expects($this->once())
            ->method('has')
            ->with($this->model::TARGETRULE_MODULE)
            ->will($this->returnValue(false));
        $this->assertFalse($this->model->getTargetRulesEnabled());
    }

    /**
     * We want a real instance of the class so that the repository under test
     * can set the data and then we can get() it and compare it with a real
     * expected instance on which we also use get and set
     *
     * @return object
     */
    private function getNewRealTargetRuleProductsObject()
    {
        // Generate the mocks and stubs for the constructor

        // CONTEXT MOCK
        $eventManagerMock = $this->createPartialMock(
            \Magento\Framework\Event\ManagerInterface::class,
            ['dispatch']
        );
        $actionValidatorMock = $this->createMock(
            \Magento\Framework\Model\ActionValidator\RemoveAction::class
        );
        $actionValidatorMock->expects($this->any())->method('isAllowed')->will($this->returnValue(true));
        $cacheInterfaceMock = $this->createMock(\Magento\Framework\App\CacheInterface::class);
        // And set the area code to backend html (do we need this?)
        $appStateMock = $this->createPartialMock(
            \Magento\Framework\App\State::class,
            ['getAreaCode', 'isAreaCodeEmulated']
        );
        $appStateMock->expects($this->any())
            ->method('getAreaCode')
            ->will($this->returnValue(\Magento\Backend\App\Area\FrontNameResolver::AREA_CODE));

        $contextMock = $this->createPartialMock(
            \Magento\Framework\Model\Context::class,
            ['getEventDispatcher', 'getCacheManager', 'getAppState', 'getActionValidator'], [], '', false
        );
        $contextMock->expects($this->any())->method('getAppState')->will($this->returnValue($appStateMock));
        $contextMock->expects($this->any())
            ->method('getEventDispatcher')
            ->will($this->returnValue($eventManagerMock));
        $contextMock->expects($this->any())
            ->method('getCacheManager')
            ->will($this->returnValue($cacheInterfaceMock));
        $contextMock->expects($this->any())
            ->method('getActionValidator')
            ->will($this->returnValue($actionValidatorMock));
        // END CONTEXT MOCK

        // REGISTRY MOCK
        $registry = $this->getMockBuilder(\Magento\Framework\Registry::class)
            ->disableOriginalConstructor()
            ->getMock();
        // END REGISTRY MOCK

        // EXTENSION ATTRIBUTES FACTORY MOCK
        $extensionAttributes = $this->getMockBuilder(
            \Acquia\CommerceManager\Api\Data\TargetRuleProductsExtensionInterface::class)
            // do we need to stub? mock should suffice, no?
            // No. possibly no code generation now.
            // But if we stub these, what are we really testing?
            ->setMethods(['setData','getData'])
            // Some kind of magic:
            ->getMockForAbstractClass();
        $extensionAttributesFactory = $this->getMockBuilder(\Magento\Framework\Api\ExtensionAttributesFactory::class)
            ->disableOriginalConstructor()
            ->getMock();
        $extensionAttributesFactory
            ->expects($this->any())
            ->method('create')
            ->willReturn($extensionAttributes);
        // END EXTENSION ATTRIBUTES FACTORY MOCK

        // ATTRIBUTE VALUE FACTORY
        $attributeValueFactory = $this->getMockBuilder(\Magento\Framework\Api\AttributeValueFactory::class)
            ->disableOriginalConstructor()->getMock();
        // END ATTRIBUTE VALUE FACTORY

        $data = [];

        // We want an *almost* real one.
        // We want to wrap with mock to expect function call counts
        // on getExtensionAttributes and afterGetRelatedProducts()
        // TODO (Malachy): Make this work:
        // Expect to call once: (these are requirements)
        // That is nice. But you are using a *real* TargetRuleProducts object
        // so you can't expect anything of it.
        /*
        $newRealTargetRuleProductsObject->expects($this->once())
            ->method("getExtensionAttributes");
        $newRealTargetRuleProductsObject->expects($this->once())
            ->method("afterGetRelatedProductsByType");
        */

        // Generate the new real object
        $newRealTargetRuleProductsObject = $this->objectManager->getObject(
            \Acquia\CommerceManager\Model\TargetRuleProducts::class,
            [
                'context' => $contextMock,
                'registry' => $registry,
                'extensionFactory' => $extensionAttributesFactory,
                'customAttributeFactory' => $attributeValueFactory,
                'data' => $data
            ]
        );

        return $newRealTargetRuleProductsObject;
    }

    public function testGetRelatedProductByTypeReturnsEmptyWhenNotEnabled()
    {
        $sku = "someSku";
        $type = "all";

        // Testing the response when the dependent module is absent.
        $this->moduleListMock->expects($this->once())
            ->method('has')
            ->with($this->model::TARGETRULE_MODULE)
            ->will($this->returnValue(false));

        // Do it.
        $result = $this->model->getRelatedProductsByType($sku, $type);

        // Check it.
        $this->assertEquals($sku, $result->getSku());
        $this->assertEquals($type, $result->getType());
        $this->assertEquals([], $result->getCrosssell());
        $this->assertEquals([], $result->getUpsell());
        $this->assertEquals([], $result->getRelated());
    }

    public function getNewProductMock($data)
    {
        return $this->createConfiguredMock(\Magento\Catalog\Model\Product::class,$data);
    }

    public function testGetRelatedProductByTypeReturnsArraysOfSkus()
    {
        $sku = "someSku";
        $type = "all";

        // EXPECTATIONS
        // We use the same SKU in each set because we mock (stub) collection->create()
        // so we can only return one collection (the same collection)
        // or, well, we could hoop jump to return different things each time collection->create()
        // is called but we aren't testing collection->create()...
        $expect = [
            "sku_1636",
            "sku_1637",
            "sku_1638"
        ];

        // Testing the response when the dependent module is present.
        $this->moduleListMock->expects($this->once())
            ->method('has')
            ->with($this->model::TARGETRULE_MODULE)
            ->will($this->returnValue(true));

        // Mock fetching the product whose relations we seek.
        $this->productRepositoryMock->expects($this->once())
            ->method("get")
            ->with($sku)
            ->willReturn($this->getNewProductMock(["getSku"=>$sku]));

        // Mock the target rule indexer.
        // We already created the mock the indexer in SetUp(), so no code needed here for that.

        // Add test data to the productCollectionMock's iterator and make its factoryMock return it.
        // This is the pattern for filling a collection with things to loop over
        // But see also $this->objectManager->getCollectionMock($classname,$data) [same thing]
        $this->productCollectionMock->method('getIterator')
            ->willReturn(new \ArrayIterator(array(
                $this->getNewProductMock(["getSku"=>"sku_1636"]),
                $this->getNewProductMock(["getSku"=>"sku_1637"]),
                $this->getNewProductMock(["getSku"=>"sku_1638"])
            )));

        // Create a collection three times (once each for cross, up and related)
        $this->productCollectionFactoryMock->expects($this->exactly(3))
            ->method("create")
            ->withAnyParameters()
            ->willReturn($this->productCollectionMock);

        // Function-call expectations.
        // Are these requirements of TDD or are we just testing what we wrote?
        // Or do you say we are testing the private function? (but it is still a weak test and
        // over-constrains the implementation)
        $this->productCollectionMock->expects($this->exactly(3))
            ->method('addFieldToFilter')
            ->with('entity_id', ['in' => $this->arbitraryProductIds])
            ->willReturn($this->productCollectionMock);
        $this->productCollectionMock->expects($this->exactly(3))
            ->method('setPageSize')
            ->with(\Magento\TargetRule\Helper\Data::MAX_PRODUCT_LIST_RESULT)
            ->willReturn($this->productCollectionMock);
        $this->productCollectionMock->expects($this->exactly(3))
            ->method('setFlag')
            ->with('do_not_use_category_id', true)
            ->willReturn($this->productCollectionMock);
        $this->productCollectionMock->expects($this->exactly(3))
            ->method('setVisibility')
            //->with($this->visibilityMock->getVisibleInCatalogIds())
            ->willReturn($this->productCollectionMock);

        // Do it.
        $result = $this->model->getRelatedProductsByType($sku, $type);

        // Check it.
        $this->assertEquals($sku, $result->getSku());
        $this->assertEquals($type, $result->getType());
        $this->assertEquals($expect, $result->getCrosssell());
        $this->assertEquals($expect, $result->getUpsell());
        $this->assertEquals($expect, $result->getRelated());
    }

}
