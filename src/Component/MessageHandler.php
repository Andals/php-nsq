<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 11:40
 */

namespace PhpNsq\Component;

use PhpNsq\Frame\Message;

interface MessageHandler
{

    /**
     * @param Message $message
     * @return bool
     */
    public function handleMessage(Message $message);
}