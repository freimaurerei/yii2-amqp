<?php

namespace freimaurerei\yii2\amqp\controllers;

use freimaurerei\yii2\amqp\AMQP;
use yii\console\Controller;
use yii\base\InvalidConfigException;
use yii\log\Logger;

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
     * Time to sleep
     * @var int
     */
    public $tts = 1;

    /**
     * Retry count
     * @var int
     */
    public $maxRetryCount = 10;

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
            $this->queueName = preg_replace('/(\w+)Controller/', '\\1', static::class);
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
        $queue = $this->amqp->getQueue($this->queueName);
        $tts = $this->tts;
        $retryCount = 0;
        if ($queue) {
            while ($retryCount < $this->maxRetryCount) {
                $queue->consume([$this, 'handleMessage']);
                sleep($tts);
                $tts <<= 1;
            }
        }
    }

    /**
     * Message handler
     * @param \AMQPEnvelope $envelope
     * @param \AMQPQueue    $queue
     * @return bool
     */
    public function handleMessage(\AMQPEnvelope $envelope, \AMQPQueue $queue)
    {
        if ($this->actionRunJob(\yii\helpers\Json::decode($envelope->getBody()))) {
            $queue->ack($envelope->getDeliveryTag());
            AMQP::$logger->log(json_encode([
                'data'  => $envelope->getBody(),
                'route' => $envelope->getRoutingKey(),
                'status'  => AMQP::MESSAGE_STATUS_ACK
            ]), Logger::LEVEL_INFO, AMQP::$logCategory);
            return true;
        } else {
            $queue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE);
            AMQP::$logger->log(json_encode([
                'data'  => $envelope->getBody(),
                'route' => $envelope->getRoutingKey(),
                'status'  => AMQP::MESSAGE_STATUS_NACK
            ]), Logger::LEVEL_INFO, AMQP::$logCategory);
            return false;
        }
    }

    /**
     * Handler
     * @param array $args
     * @return bool
     */
    abstract protected function actionRunJob($args) : bool;
}