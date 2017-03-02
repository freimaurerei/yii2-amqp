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

class TestQueueListener extends \freimaurerei\yii2\amqp\controllers\QueueListener
{

    public function actionRunJob(...$args) : bool
    {
        var_dump($args);
        return true;
    }
}

$module = new \yii\base\Module(1, null, [
]);

$listener = new TestQueueListener('test', $module, [
    'amqp' => $amqp,
    'queueName' => 'test_queue',
]);
$listener->actionRun();