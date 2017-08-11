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
    public function handleMessage(Message $message);
}