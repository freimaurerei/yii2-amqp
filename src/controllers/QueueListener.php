<?php

namespace freimaurerei\yii2\amqp\controllers;

use freimaurerei\yii2\amqp\actions\QueueAction;
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
     * Actions config
     * Example:
     * $actionConfig = [
     *      'actionImport' => [
     *          'maxRetryCount' => 10,
     *          'waitingTime' => 100,
     *      ],
     *      'actionDoingSomething' => [
     *          'maxRetryCount' => 5,
     *          'waitingTime' => 0
     *      ],
     * ];
     * @var array
     */
    public $actionsConfig = [];

    /**
     * Default config for actions
     */
    const DEFAULT_ACTION_CONFIG = [
        'maxRetryCount' => 5,
        'waitingTime' => 10,
    ];

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
     * @var bool
     */
    public $useDelayQueue = false;

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
     * @inheritdoc
     */
    public function createAction($id)
    {
        if ($id === '') {
            $id = $this->defaultAction;
        }

        $actionMap = $this->actions();
        if (isset($actionMap[$id])) {
            return \Yii::createObject($actionMap[$id], [$id, $this]);
        } elseif (preg_match('/^[a-z0-9\\-_]+$/', $id) && strpos($id, '--') === false && trim($id, '-') === $id) {
            $methodName = 'action' . str_replace(' ', '', ucwords(implode(' ', explode('-', $id))));
            if (method_exists($this, $methodName)) {
                $method = new \ReflectionMethod($this, $methodName);
                if ($method->isPublic() && $method->getName() === $methodName) {
                    return new QueueAction($id, $this, $methodName, ['amqp' => $this->amqp]);
                }
            }
        }

        return null;
    }

    public function getActionConfig($action)
    {
        return isset($this->actionsConfig[$action]) ? $this->actionsConfig[$action] : static::DEFAULT_ACTION_CONFIG;
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
}