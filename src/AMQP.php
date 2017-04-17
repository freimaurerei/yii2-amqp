<?php

namespace freimaurerei\yii2\amqp;

use yii\base\Component;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;

/**
 * Class AMQP
 * @package freimaurerei\yii2\amqp
 */
class AMQP extends Component
{

    /**
     * Constants for a tracing a message status in RabbitMQ
     */
    const MESSAGE_STATUS_ADDED = 0;
    const MESSAGE_STATUS_HANDLED = 1;
    const MESSAGE_STATUS_ACK = 2;
    const MESSAGE_STATUS_NACK = 3;

    /** @var string $host */
    public $host;
    /** @var string $port */
    public $port;
    /** @var string $vhost */
    public $vhost;
    /** @var string $login */
    public $login;
    /** @var string $password */
    public $password;
    /** @var array $config */
    public $config = [];
    /** @var bool */
    public $delayQueueUsage = false;
    /** @var string  */
    public $delayedExchangeName = 'delayed-exchange';
    /** @var string */
    public $delayedQueueName = 'delayed-queue';
    /** @var string */
    public static $logCategory = __NAMESPACE__;

    /** @var \AMQPConnection $connection */
    protected $connection = null;
    /** @var \AMQPChannel[] $channels */
    protected $channels = null;
    /** @var \AMQPQueue[] $queues */
    protected $queues = [];
    /** @var \AMQPExchange[] $exchanges */
    protected $exchanges = [];

    /** @var \AMQPQueue */
    private $delayedQueue = null;
    /** @var \AMQPExchange */
    private $delayedExchange = null;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $connection = new \AMQPConnection();
        $connection->setHost($this->host);
        $connection->setPort($this->port);
        $connection->setVhost($this->vhost);
        $connection->setLogin($this->login);
        $connection->setPassword($this->password);

        if ($connection->connect()) {
            $this->connection = $connection;
        } else {
            throw new \RuntimeException('Can\'t connect to AMQP');
        }

        if ($this->delayQueueUsage) {
            $this->makeDelayedQueue();
        }
    }

    /**
     * Returns AMQP connection
     * @return \AMQPConnection
     */
    public function getConnection(): \AMQPConnection
    {
        return $this->connection;
    }

    /**
     * @param      $exchangeName
     * @param string|null $channelId
     * @return \AMQPExchange
     */
    public function getExchange($exchangeName, $channelId = null): \AMQPExchange
    {
        $exchange = $this->makeExchange($exchangeName, $channelId);

        $aQueues = $this->getQueuesByExchangeName($exchangeName);
        foreach ($aQueues as $queue) {
            $this->makeQueue($queue, $channelId);
        }

        return $exchange;
    }

    /**
     * @param      $queueName
     *
     * @param string|null $channelId
     * @return \AMQPQueue
     */
    public function getQueue($queueName, $channelId = null): \AMQPQueue
    {
        $aExchanges = $this->getExchangesByQueueName($queueName);
        foreach ($aExchanges as $exchangeName) {
            $this->makeExchange($exchangeName, $channelId);
        }

        $queue = $this->makeQueue($queueName, $channelId);

        return $queue;
    }

    /**
     * @param null $channelId
     * @return \AMQPChannel|null
     */
    public function getChannel($channelId = null): \AMQPChannel
    {
        if (!$channelId) {
            $channelId = 'default';
        }
        if (isset($this->channels[$channelId])) {
            return $this->channels[$channelId];
        }
        $channel   = $this->createAMQPChannel($channelId);
        return $channel;
    }

    /**
     * @return \AMQPChannel
     */
    public function getRandomChannel(): \AMQPChannel
    {
        return array_rand($this->channels);
    }

    /**
     * @param string|null $channelId
     * @return \AMQPChannel
     */
    public function createAMQPChannel($channelId = null): \AMQPChannel
    {
        $channel                    = new \AMQPChannel($this->connection);
        $channelId                  = $channelId ?? $channel->getChannelId();
        $this->channels[$channelId] = $channel;
        return $channel;
    }

    /**
     * @param string      $exchangeName
     * @param null|string $channelId
     * @return \AMQPExchange
     */
    protected function makeExchange(string $exchangeName, $channelId = null): \AMQPExchange
    {
        if (!isset($this->exchanges[$exchangeName])) {
            if (!isset($this->config['exchanges'][$exchangeName]) && $exchangeName !== '') {
                throw new \RuntimeException("Could not find the exchange '$exchangeName' in config");
            }

            $exchange = new \AMQPExchange($this->getChannel($channelId));

            if (isset($this->config['exchanges'][$exchangeName])) {
                $exchange->setName($exchangeName);
                $exchangeData = $this->config['exchanges'][$exchangeName];
                $exchange->setType($exchangeData['config']['type']);
                $exchange->setFlags($exchangeData['config']['flags']);
                $exchange->setArguments($exchangeData['config']['arguments'] ?? []);
                $exchange->declareExchange();
            }

            $this->exchanges[$exchangeName] = $exchange;
        }

        return $this->exchanges[$exchangeName];
    }

    /**
     * @param      $queueName
     *
     * @param string|null $channelId
     * @return \AMQPQueue
     */
    protected function makeQueue($queueName, $channelId = null): \AMQPQueue
    {
        if (!isset($this->queues[$queueName])) {
            if (!isset($this->config['queues'][$queueName])) {
                throw new \RuntimeException("Could not find the queue '$queueName' in config");
            }
            $queueData = $this->config['queues'][$queueName];

            $queue = new \AMQPQueue($this->getChannel($channelId));
            $queue->setName($queueName);
            $queue->setFlags($queueData['config']['flags']);
            $queue->setArguments($queueData['config']['arguments'] ?? []);
            $queue->declareQueue();

            foreach ($queueData['binds'] as $exchangeName => $aRoutingKeys) {
                if (!isset($this->exchanges[$exchangeName])) {
                    $this->makeExchange($exchangeName);
                }
                foreach ($aRoutingKeys as $routing_key) {
                    $queue->bind($exchangeName, $routing_key);
                }
            }

            $this->queues[$queueName] = $queue;
        }

        return $this->queues[$queueName];
    }

    /**
     * @param $exchangeName
     *
     * @return array
     */
    protected function getQueuesByExchangeName($exchangeName): array
    {
        $aQueues = array();
        foreach ($this->config['queues'] as $queueName => $queueData) {
            $exchanges = array_keys($queueData['binds']);
            if (in_array($exchangeName, $exchanges)) {
                $aQueues[] = $queueName;
            }
        }

        return $aQueues;
    }

    /**
     * @param $queueName
     *
     * @return array
     */
    protected function getExchangesByQueueName($queueName): array
    {
        $aExchanges = array_keys($this->config['queues'][$queueName]['binds']);

        return $aExchanges;
    }

    /**
     * Applies headers in message properties.
     *
     * @param array $properties
     * @param array $headers
     */
    protected function applyPropertyHeaders(array &$properties, array $headers = null)
    {
        if ($headers !== null) {
            $properties['headers'] = $headers;
        }
    }

    protected function makeDelayedQueue($channelId = null)
    {
        $exchange = new \AMQPExchange($this->getChannel($channelId));
        $exchange->setName($this->delayedExchangeName);
        $exchange->setType('x-delayed-message');
        $exchange->setFlags(\AMQP_DURABLE);
        $exchange->setArgument('x-delayed-type', \AMQP_EX_TYPE_FANOUT);
        $exchange->declareExchange();
        $this->delayedExchange = $exchange;

        $queue = new \AMQPQueue($this->getChannel($channelId));
        $queue->setName($this->delayedQueueName);
        $queue->setFlags(\AMQP_DURABLE);
        $queue->declareQueue();
        $this->delayedQueue = $queue;

        $queue->bind($exchange->getName());
    }

    /**
     * Send message by routingKey
     * @param string      $exchange
     * @param string      $routingKey
     * @param             $message
     * @param array|null  $headers
     * @param string|null $channelId
     * @return bool
     * @throws \ErrorException
     */
    public function send(string $exchange, string $routingKey, $message, array $headers = null, $channelId = null): bool
    {
        if ($message === null || $message === '') {
            throw new \ErrorException('AMQP message can not be empty');
        }
        if (is_array($message) || is_object($message)) {
            $message = Json::encode($message);
        }

        $properties = [
            'delivery_mode' => 2
        ];
        $this->applyPropertyHeaders($properties, $headers);
        $exchange = $this->getExchange($exchange, $channelId);
        if ($exchange->publish($message, $routingKey, AMQP_NOPARAM, $properties)) {
            \Yii::info(json_encode([
                'data'   => $message,
                'route'  => $routingKey,
                'status' => self::MESSAGE_STATUS_ADDED
            ]), self::$logCategory);
            return true;
        }
        return false;
    }

    /**
     * @param string $queueName
     * @param        $callback
     * @param bool   $break
     * @param string|null   $channelId
     */
    public function listenQueue(string $queueName, $callback, bool $break = false, $channelId = null)
    {
        $queue = $this->getQueue($queueName, $channelId);
        while (true) {
            if (($message = $queue->get()) instanceof \AMQPEnvelope) {
                if (call_user_func_array($callback, \yii\helpers\Json::decode($message->getBody()))) {
                    $queue->ack($message->getDeliveryTag());
                } else {
                    $queue->nack($message->getDeliveryTag());
                }
            } elseif ($break) {
                break;
            }
        }

        $this->getChannel($channelId)->getConnection()->disconnect();
    }

    public function beginTransaction($channels = null)
    {
        $channels = $this->getTransactionalChannels($channels);

        $transaction = new Transaction($channels);
        $transaction->begin();

        return $transaction;
    }

    /**
     * @param \AMQPChannel[]|\AMQPChannel|null $channels
     * @return \AMQPChannel[]|array|null
     */
    private function getTransactionalChannels($channels = null)
    {
        if ($channels) {
            if (!is_array($channels)) {
                $channels = [$channels];
            }
        } else {
            $channels = [$this->getChannel()];
        }

        return $channels;
    }

    /**
     * @param string $exchange
     * @return array
     */
    public function getExchangeConfig(string $exchange): array
    {
        return ArrayHelper::getValue($this->config, $exchange, []);
    }

    /**
     * @return \AMQPQueue
     */
    public function getDelayedQueue()
    {
        return $this->delayedQueue;
    }

    /**
     * @return \AMQPExchange
     */
    public function getDelayedExchange()
    {
        return $this->delayedExchange;
    }

}
