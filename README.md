# Acquia Commerce Manager Magento 2.x Extension
[![Build Status](https://travis-ci.org/acquia/commerce-manager-magento.svg?branch=master)](https://travis-ci.org/acquia/commerce-manager-magento)

A Magento 2.x extension that works the the Acuqia Commerce Connector Service to enable a seamless integration of your Magneto 2.x stores with Drupal for rapid development of digital eCommerce experiences.


## Install instructions
1. Use composer to require the extension in your Magento 2 project. From your magento 2 project root run:
```
composer require acquia/commerce-manager-magento:"~2.1"
```
2. (Magento EE only) Setup cron job to consume messages for pushing products in background.
  Cron job example: `*/5 * * * * timeout 290s php bin/magento queue:consumers:start --max-messages=100 connectorProductPushConsumer`
3. (Magento EE only) Setup cron job to consume messages for pushing stock changes.
  Cron job example: `*/5 * * * * timeout 290s php bin/magento queue:consumers:start --max-messages=100 connectorStockPushConsumer`


## Enable connector integration
1. Enable plugin:
    ```
    magento module:enable Acquia_CommerceManager
    ```
2. Enable integration itself, which will generate OAuth keys for connector:
    ```
    magento acquia:enable-integration
    ```
3. Configure the Connector in the admin section under **Stores** > **Configuration** > **Services** > **Magento Web API**.


## Swagger API documentation

The API endpoints added by this extension can be observed by bulding the Magento Swagger file in the normal way. That is, use a browser to render the URL
```text
https://your-magento-domain-name/swagger
```

The page will render the publicly accessible API endpoints. To see all the endpoints including the Aquia Commerce Manager Magento Module paths you need to enter an authorisation token in the input box on the top-right of the page.

You can get an authorization token by running
```text
curl -XPOST -H 'Content-Type: application/json' http://your-magento-domain-name/index.php/rest/V1/integration/admin/token -d '{ "username": "admin", "password": "password1" }'
``` 
With your username and password in place. Copy the returned token into the box on the /swagger page and press the `apply` button.

Please see [Magento swagger developer docs](http://devdocs.magento.com/guides/v2.0/rest/generate-local.html) for additional information about the Magento API spec.


## Development

To run the unit tests install this extension into your Magento project then run
```
vendor/bin/phpunit -c dev/tests/unit/phpunit.xml.dist vendor/acquia/commerce-manager-magento/Test/Unit
```
or use
```
vendor/bin/phpunit -c dev/tests/unit/phpunit.xml.dist app/code/Acquia/CommerceManager/Test/Unit
```
if you installed the module under the `app/code/` directory.
Run those commands from the Magento install root.

## Copyright and license

Copyright © 2018 Acquia Inc.

Each source file included in this distribution is licensed under OSL 3.0

http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
Please see file [LICENSE](https://github.com/acquia/commerce-manager-magento/blob/master/LICENSE) for the full text of the OSL 3.0
