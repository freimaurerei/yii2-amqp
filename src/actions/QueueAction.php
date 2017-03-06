<?php

namespace freimaurerei\yii2\amqp\actions;

use freimaurerei\yii2\amqp\AMQP;
use freimaurerei\yii2\amqp\controllers\QueueListener;
use yii\base\InlineAction;
use yii\base\NotSupportedException;

class QueueAction extends InlineAction
{

    public function __construct($id, $controller, $actionMethod, $config = [])
    {
        if (!$controller instanceof QueueListener) {
            throw new NotSupportedException();
        }

        parent::__construct($id, $controller, $actionMethod, $config);
    }

    /**
     * Runs this action with the specified parameters.
     * This method is mainly invoked by the controller.
     * @param array $params action parameters
     * @return mixed the result of the action
     */
    public function runWithParams($params)
    {
        // todo think what to do with args
        $args = [];

        \Yii::trace('Running action: ' . get_class($this->controller) . '::' . $this->actionMethod . '()', __METHOD__);
        if (\Yii::$app->requestedParams === null) {
            \Yii::$app->requestedParams = $args;
        }

        /** @var QueueListener $controller */
        $controller = $this->controller;
        $queueName  = get_class($this->controller) . '::' . $this->actionMethod;
        $queue      = $controller->amqp->getQueue($queueName);
        $tts        = $controller->tts;
        $retryCount = 0;
        if ($queue) {
            while ($retryCount < $controller->maxRetryCount) {
                $queue->consume([$this, 'handleMessage']);
                sleep($tts);
                $tts <<= 1;
            }
        }

        return 0;
    }

    /**
     * Routing key must be $className::$actionName
     * Message handler
     * @param \AMQPEnvelope $envelope
     * @param \AMQPQueue    $queue
     * @return bool
     */
    public function handleMessage(\AMQPEnvelope $envelope, \AMQPQueue $queue)
    {
        \Yii::info("Handled message: " . $envelope->getBody());

        if ($this->controller->{$this->actionMethod}(\yii\helpers\Json::decode($envelope->getBody()))) {
            $queue->ack($envelope->getDeliveryTag());
            \Yii::info(json_encode([
                'data'   => $envelope->getBody(),
                'route'  => $envelope->getRoutingKey(),
                'status' => AMQP::MESSAGE_STATUS_ACK
            ]), AMQP::$logCategory);
            return true;
        } else {
            $queue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE);
            \Yii::info(json_encode([
                'data'   => $envelope->getBody(),
                'route'  => $envelope->getRoutingKey(),
                'status' => AMQP::MESSAGE_STATUS_NACK
            ]), AMQP::$logCategory);
            return false;
        }
    }
}