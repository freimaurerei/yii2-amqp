<?php

namespace freimaurerei\yii2\amqp\controllers;

use freimaurerei\yii2\amqp\AMQP;
use yii\base\InvalidConfigException;
use yii\console\Controller;

class DelayedMessageHandler extends Controller
{
    /**
     * AMQP component
     * @var AMQP|string
     */
    public $amqp = 'amqp';

    /**
     * @inheritdoc
     * @throws InvalidConfigException
     */
    public function init()
    {
        parent::init();
        if (is_string($this->amqp)) {
            $this->amqp = \Yii::$app->get($this->amqp);
        }
        if (!($this->amqp instanceof AMQP)) {
            throw new InvalidConfigException(
                sprintf('AMQP component must be defined and be instance of "%s".', AMQP::class)
            );
        }
    }

    /**
     * Delayed messages handler
     */
    public function actionIndex()
    {
        $queue = $this->amqp->getDelayedQueue();
        if ($queue) {
            while (true) {
                $queue->consume([$this, 'handleMessage']);
            }
        }
    }

    public function handleMessage(\AMQPEnvelope $envelope, \AMQPQueue $queue)
    {
        $this->amqp->send('', $envelope->getRoutingKey(), $envelope->getBody(), $envelope->getHeaders());
    }
}
