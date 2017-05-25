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
     * todo description
     * @var int
     */
    protected $maxRetryCount = 10;

    /**
     * todo description
     * Boundary retry count
     * @var int
     */
    protected $retryBoundaryCount = 10;

    /**
     * In seconds
     * @var int
     */
    protected $waitingTime = 10;

    public function __construct($id, $controller, $actionMethod, $config = [])
    {
        if (!$controller instanceof QueueListener) {
            throw new NotSupportedException();
        }

        $actionConfig = $controller->getActionConfig($actionMethod);
        if (isset($actionConfig['maxRetryCount'])) {
            $this->maxRetryCount = $actionConfig['maxRetryCount'];
        }
        if (isset($actionConfig['waitingTime'])) {
            $this->waitingTime = $actionConfig['waitingTime'];
        }
        if (isset($actionConfig['retryBoundaryCount']) && $actionConfig['retryBoundaryCount'] <= $this->maxRetryCount) {
            $this->retryBoundaryCount = $actionConfig['retryBoundaryCount'];
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
        $queue      = $controller->amqp->getQueue($queueName, __CLASS__);
        if ($queue) {
            switch ($controller->mode) {
                case QueueListener::MODE_DAEMON:
                    while (true) {
                        $queue->consume([$this, 'handleMessage']); // todo think about this situation
                    }
                    break;
                case QueueListener::MODE_NODAEMON:
                    $continue = true;
                    while ($continue && ($envelope = $queue->get()) !== false) {
                        $continue = $this->handleMessage($envelope, $queue);
                    }
                    break;
            }
        }

        return 0;
    }

    public function bindActionParams($params)
    {
        if (!is_array($params)) {
            return false;
        }
        $method = new \ReflectionMethod($this->controller, $this->actionMethod);

        $args = [];
        $missing = [];
        $actionParams = [];
        foreach ($method->getParameters() as $param) {
            $name = $param->getName();
            if (array_key_exists($name, $params)) {
                if ($param->isArray()) {
                    $args[] = $actionParams[$name] = (array) $params[$name];
                } else {
                    $args[] = $actionParams[$name] = $params[$name];
                }
                unset($params[$name]);
            } elseif ($param->isDefaultValueAvailable()) {
                $args[] = $actionParams[$name] = $param->getDefaultValue();
            } else {
                $missing[] = $name;
            }
        }

        if (!empty($missing)) {
            \Yii::info(\Yii::t('yii', 'Missing required parameters: {params}', [
                'params' => implode(', ', $missing),
            ]), AMQP::$logCategory);
            return false;
        }

        return $args;
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

        $args = $this->bindActionParams(\yii\helpers\Json::decode($envelope->getBody()));

        if ($args === false) {
            return false;
        }

        // good
        $result = call_user_func_array(
            [$this->controller, $this->actionMethod],
            $args
        );
        if ($result) {
            \Yii::info(json_encode([
                'data'   => $envelope->getBody(),
                'route'  => $envelope->getRoutingKey(),
                'status' => AMQP::MESSAGE_STATUS_ACK
            ]), AMQP::$logCategory);
        } else {
            ++$redeliveredCount;
            if ($this->amqp->delayQueueUsage) {
                if ($redeliveredCount > $this->maxRetryCount) {
                    \Yii::info("Message could not be processed {$this->retryCount} times. The message was deleted."
                        . json_encode([
                            'data'   => $envelope->getBody(),
                            'route'  => $envelope->getRoutingKey(),
                            'status' => AMQP::MESSAGE_STATUS_ACK
                        ]), AMQP::$logCategory);
                } else {
                    $delayedTime = null;
                    if ($this->retryBoundaryCount <= $redeliveredCount) {
                        $delayedTime = $envelope->getHeader('x-delay');
                    }

                    $delayedTime = $delayedTime ?: ($this->waitingTime * (1 << $redeliveredCount)) * 1000;

                    $this->amqp->getDelayedExchange()->publish(
                        $envelope->getBody(),
                        $queue->getName(),
                        AMQP_NOPARAM,
                        [
                            'headers' => [
                                'x-delay'             => $delayedTime,
                                'delivery_mode'       => 2,
                                'x-redelivered-count' => $redeliveredCount
                            ]
                        ]
                    );
                }
            } else {
                $this->amqp->send(
                    '',
                    $queue->getName(),
                    $envelope->getBody(),
                    [
                        'x-redelivered-count' => $redeliveredCount
                    ]
                );
                \Yii::info(json_encode([
                    'data'   => $envelope->getBody(),
                    'route'  => $envelope->getRoutingKey(),
                    'status' => AMQP::MESSAGE_STATUS_NACK
                ]), AMQP::$logCategory);
            }
        }
        $queue->ack($envelope->getDeliveryTag());

        return $result;
    }
}
