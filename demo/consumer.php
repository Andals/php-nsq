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
use PhpNsq\Log\Formater\General as GeneralFormater;
use PhpNsq\Log\Writer\File as FileWriter;

require_once(__DIR__ . '/../vendor/autoload.php');

class MyMessageHandler implements MessageHandler
{
    const MSG_CNT_TO_REQ = 10;

    private $msgCnt = 0;

    public function handleMessage(Message $message)
    {
        // TODO: Implement handleMessage() method.
        echo $message->getMessageBody() . "\n";

        $this->msgCnt++;
        if ($this->msgCnt === self::MSG_CNT_TO_REQ) {
            $this->msgCnt = 0;

            return false;
        }

        return true;
    }
}

$formater = new GeneralFormater();
$writer   = new FileWriter('/tmp/php_nsq_demo.log');
$logger   = new \PhpNsq\Log\Logger($formater, $writer);

$consumer = new Consumer();
$consumer->messageNeedGunzip()
         ->addNsqlookupd('http://127.0.0.1:4161')
         ->setHandler(new MyMessageHandler())
         ->setLogger($logger);
try {
    $consumer->run('app2_t2', 'c1');
} catch (Exception $e) {
    var_dump($e->getMessage());
}