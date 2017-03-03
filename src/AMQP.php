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
     * @param $exchangeName
     *
     * @return \AMQPExchange
     */
    public function getExchange($exchangeName): \AMQPExchange
    {
        $exchange = $this->makeExchange($exchangeName);

        $aQueues = $this->getQueuesByExchangeName($exchangeName);
        foreach ($aQueues as $queue) {
            $this->makeQueue($queue);
        }

        return $exchange;
    }

    /**
     * @param $queueName
     *
     * @return \AMQPQueue
     */
    public function getQueue($queueName): \AMQPQueue
    {
        $aExchanges = $this->getExchangesByQueueName($queueName);
        foreach ($aExchanges as $exchangeName) {
            $this->makeExchange($exchangeName);
        }

        $queue = $this->makeQueue($queueName);

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
            $this->createAMQPChannel($channelId);
        }
        return $this->channels[$channelId] ?? null;
    }

    /**
     * @return \AMQPChannel
     */
    public function getRandomChannel(): \AMQPChannel
    {
        return array_rand($this->channels);
    }

    /**
     * @param null $channelId
     * @return \AMQPChannel
     */
    public function createAMQPChannel($channelId = null): \AMQPChannel
    {
        $channel = new \AMQPChannel($this->connection);
        $channelId                  = $channelId ?? $channel->getChannelId();
        $this->channels[$channelId] = $channel;
        return $channel;
    }

    /**
     * @param string $exchangeName
     * @throws \RuntimeException
     * @return \AMQPExchange
     */
    protected function makeExchange(string $exchangeName): \AMQPExchange
    {
        if (!isset($this->exchanges[$exchangeName])) {
            if (!isset($this->config['exchanges'][$exchangeName])) {
                throw new \RuntimeException("Could not find the exchange '$exchangeName' in config");
            }

            $exchangeData = $this->config['exchanges'][$exchangeName];

            $exchange = new \AMQPExchange($this->getChannel());
            $exchange->setName($exchangeName);
            $exchange->setType($exchangeData['config']['type']);
            $exchange->setFlags($exchangeData['config']['flags']);
            $exchange->setArguments($exchangeData['config']['arguments'] ?? []);

            $exchange->declareExchange();

            $this->exchanges[$exchangeName] = $exchange;
        }

        return $this->exchanges[$exchangeName];
    }

    /**
     * @param $queueName
     *
     * @return \AMQPQueue
     * @throws \RuntimeException
     */
    protected function makeQueue($queueName): \AMQPQueue
    {
        if (!isset($this->queues[$queueName])) {
            if (!isset($this->config['queues'][$queueName])) {
                throw new \RuntimeException("Could not find the queue '$queueName' in config");
            }
            $queueData = $this->config['queues'][$queueName];

            $queue = new \AMQPQueue($this->getChannel());
            $queue->setName($queueName);
            $queue->setFlags($queueData['config']['flags']);
            $queue->setArguments($queueData['config']['arguments'] ?? []);
            $queue->declareQueue();

            foreach ($queueData['binds'] as $exchangeName => $aRoutingKeys) {
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
            $properties['application_headers'] = $headers;
        }
    }

    /**
     * Send message by routingKey
     * @param string     $exchange
     * @param string     $routingKey
     * @param            $message
     * @param array|null $headers
     * @return bool
     * @throws \ErrorException
     */
    public function send(string $exchange, string $routingKey, $message, array $headers = null): bool
    {
        if ($message === null || $message === '') {
            throw new \ErrorException('AMQP message can not be empty');
        }
        if (is_array($message) || is_object($message)) {
            $message = Json::encode($message);
        }

        $properties = [];
        $this->applyPropertyHeaders($properties, $headers);
        $exchange = $this->getExchange($exchange);
        if ($exchange->publish($message, $routingKey, AMQP_NOPARAM, $properties)) {
            \Yii::info(json_encode([
                'data'  => $message,
                'route' => $routingKey,
                'status'  => self::MESSAGE_STATUS_ADDED
            ]), self::$logCategory);
            return true;
        }
        return false;
    }

    /**
     * @param string $queueName
     * @param        $callback
     * @param bool   $break
     */
    public function listenQueue(string $queueName, $callback, bool $break = false)
    {
        $queue = $this->getQueue($queueName);
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

        $this->queues[$queueName]->getChannel()->getConnection()->disconnect();
    }

    /**
     * @param string $exchange
     * @return array
     */
    public function getExchangeConfig(string $exchange): array
    {
        return ArrayHelper::getValue($this->config, $exchange, []);
    }
}