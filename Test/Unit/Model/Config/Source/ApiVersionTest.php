<?php

namespace Acquia\CommerceManager\Test\Unit\Model\Config\Source;
use Acquia\CommerceManager\Model\Config\Source\ApiVersion;

class ApiVersionTest extends \PHPUnit\Framework\TestCase
{
    public $apiVersion;

    public function setUp()
    {
        $this->apiVersion = new ApiVersion();
    }

    public function OFFtestAlwaysPass()
    {
        $this->assertTrue(true);
    }

    public function OFFtestAlwaysFail()
    {
        $this->fail("I expected this test to fail");
    }

    public function testToOptionArray()
    {
        $this->assertEquals("v1", $this->apiVersion->toOptionArray()[0]["value"],"v1 is not an option value");
        $this->assertEquals("Version one", $this->apiVersion->toOptionArray()[0]["label"],"Version one is not an option label");

        $this->assertEquals("v2",$this->apiVersion->toOptionArray()[1]["value"],"v2 is not an option value");
        $this->assertEquals("Version two", $this->apiVersion->toOptionArray()[1]["label"],"Version two is not an option label");
    }

}