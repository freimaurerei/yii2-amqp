<?php

namespace freimaurerei\yii2\amqp\controllers;

use yii\base\InlineAction;

class QueueAction extends InlineAction
{

    /**
     * Runs this action with the specified parameters.
     * This method is mainly invoked by the controller.
     * @param array $params action parameters
     * @return mixed the result of the action
     */
    public function runWithParams($params)
    {
        $args = $this->controller->bindActionParams($this, $params);
        $args['callable'] = $this->actionMethod;
        \Yii::trace('Running action: ' . get_class($this->controller) . '::' . $this->actionMethod . '()', __METHOD__);
        if (\Yii::$app->requestedParams === null) {
            \Yii::$app->requestedParams = $args;
        }

        return call_user_func_array([$this->controller, 'listenQueue'], $args); // todo do better
    }
}