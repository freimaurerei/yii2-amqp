<?php

namespace freimaurerei\yii2\amqp\actions;

use freimaurerei\yii2\amqp\AMQP;
use freimaurerei\yii2\amqp\controllers\QueueListener;
use yii\base\InlineAction;
use yii\base\NotSupportedException;

class QueueAction extends InlineAction
{
    /**
     * AMQP component
     * @var AMQP
     */
    public $amqp;

    /**
     * Retry count
     * @var int
     */
    public $retryCount = 5;

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
        if ($queue) {
            while (true) {
                $queue->consume([$this, 'handleMessage']); // todo think about this situation
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

        $redeliveredCount = $envelope->getHeader('x-redelivered-count') ?: 0;

        $result = true;

        if ($redeliveredCount < $this->retryCount) {
            // good
            $result = call_user_func_array(
                [$this->controller, $this->actionMethod],
                \yii\helpers\Json::decode($envelope->getBody())
            );
            if ($result) {
                \Yii::info(json_encode([
                    'data'   => $envelope->getBody(),
                    'route'  => $envelope->getRoutingKey(),
                    'status' => AMQP::MESSAGE_STATUS_ACK
                ]), AMQP::$logCategory);

            } else {
                $this->amqp->send('', $queue->getName(), $envelope->getBody(), ['x-redelivered-count' => ++$redeliveredCount]);
                \Yii::info(json_encode([
                    'data'   => $envelope->getBody(),
                    'route'  => $envelope->getRoutingKey(),
                    'status' => AMQP::MESSAGE_STATUS_NACK
                ]), AMQP::$logCategory);
            }
        } else {
            \Yii::info("Message could not be processed {$this->retryCount} times. The message was deleted." . json_encode([
                'data'   => $envelope->getBody(),
                'route'  => $envelope->getRoutingKey(),
                'status' => AMQP::MESSAGE_STATUS_ACK
            ]), AMQP::$logCategory);
        }

        $queue->ack($envelope->getDeliveryTag());

        return $result;
    }
}