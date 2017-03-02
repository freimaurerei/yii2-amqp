<?php

require_once(__DIR__ .  '/../vendor/autoload.php');
require_once(__DIR__ .  '/../vendor/yiisoft/yii2/Yii.php');

$amqp = new \freimaurerei\yii2\amqp\AMQP([
    "host" => 'localhost',
    'port' => '5672',
    'vhost' => '/test',
    'login' => 'guest',
    'password' => 'guest',
    'config' => [
        'exchanges' => [
            'test_exchange' => [
                'config' => [
                    'flags' => \AMQP_DURABLE,
                    'type'  => \AMQP_EX_TYPE_DIRECT,
                ]
            ],
        ],
        'queues' => [
            'test_queue' => [
                'binds' => [
                    'test_exchange' => [
                        'TestRoute',
                    ],
                ],
                'config' => [
                    'flags' => \AMQP_DURABLE,
                ]
            ],
        ],
    ],
]);

$amqp->send('test_exchange', 'TestRoute', '{"orderId": 1}');