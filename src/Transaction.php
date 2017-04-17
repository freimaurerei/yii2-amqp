<?php

namespace freimaurerei\yii2\amqp;

/**
 *
 * AMQP's transaction
 */
class Transaction
{
    /**
     * @var \AMQPChannel[]
     */
    protected $channels = [];

    /**
     * @var array
     */
    protected $transactionalChannels = [];

    /**
     * @var bool
     */
    public $isTransaction = false;

    public function __construct(array $channels)
    {
        $this->channels = $channels;
    }

    public function begin()
    {
        if ($this->channels) {
            foreach ($this->channels as $c) {
                if (!in_array($c, $this->transactionalChannels)) {
                    if (!$c->startTransaction()) {
                        $this->rollback();
                        return false;
                    }
                    $this->transactionalChannels[] = $c;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    public function commit()
    {
        foreach ($this->transactionalChannels as $i=>$c) {
            /** @var \AMQPChannel $c */
            if (!$c->commitTransaction()) {
                return false;
            }
            unset($this->transactionalChannels[$i]);
        }

        $this->isTransaction = false;

        return true;
    }

    public function rollback()
    {
        foreach ($this->transactionalChannels as $i=>$c) {
            /** @var \AMQPChannel $c */
            if (!$c->rollbackTransaction()) {
                return false;
            }
            unset($this->transactionalChannels[$i]);
        }

        $this->isTransaction = false;

        return true;
    }
}
