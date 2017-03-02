<?php

namespace freimaurerei\yii2\amqp\controllers;

use freimaurerei\yii2\amqp\AMQP;
use yii\console\Controller;
use yii\base\InvalidConfigException;

/**
 * Class QueueListener
 * @package freimaurerei\yii2\amqp\controllers
 */
abstract class QueueListener extends Controller
{
    /**
     * bind queue.
     *
     * @var string
     */
    public $queueName = '';
    /**
     * break listen
     *
     * @var boolean
     */
    public $break = false;

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
        if (!$this->queueName) {
            $this->queueName = preg_replace('/(\w+)Listener/', '\\1', static::class);
            $this->amqp->getQueue($this->queueName);
        }
    }

    /**
     * @inheritdoc
     */
    public function options($actionId)
    {
        return array_merge(
            parent::options($actionId),
            ['exchange', 'queue', 'break']
        );
    }

    /**
     * Main action
     */
    public function actionRun()
    {
        $this->amqp->listenQueue($this->queueName, [$this, 'actionRunJob']);
    }

    /**
     * Handler
     * @param array ...$args
     * @return bool
     */
    abstract public function actionRunJob(...$args) : bool;
}