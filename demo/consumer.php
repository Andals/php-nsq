<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 16:26
 */

use PhpNsq\Component\Consumer;
use PhpNsq\Component\MessageHandler;
use PhpNsq\Frame\Message;

require_once(__DIR__.'/../vendor/autoload.php');

class MyMessageHandler implements MessageHandler
{
    public function handleMessage(Message $message)
    {
        // TODO: Implement handleMessage() method.
        var_dump($message->getMessageBody());
    }
}

$consumer = new Consumer();
$consumer->messageNeedGunzip();
$consumer->addNsqlookupd('http://127.0.0.1:4161');
$consumer->setHandler(new MyMessageHandler());
$consumer->run('app2_t2', 'c1');
