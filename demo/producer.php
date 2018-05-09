<?php
/**
 * Created by IntelliJ IDEA.
 * User: tabalt
 * Date: 2017/9/14
 * Time: 15:20
 */

use PhpNsq\Component\Producer;
use PhpNsq\Component\MessageHandler;
use PhpNsq\Frame\Message;
use PhpNsq\Log\Formater\General as GeneralFormater;
use PhpNsq\Log\Writer\File as FileWriter;

require_once(__DIR__ . '/../vendor/autoload.php');

$formater = new GeneralFormater();
$writer   = new FileWriter('/tmp/php_nsq_demo.log');
$logger   = new \PhpNsq\Log\Logger($formater, $writer);

$producer = new Producer();
$producer->messageNeedGzip()
         ->addNsqlookupd('http://127.0.0.1:4161')
         ->setTimeout(array('sec' => 10,'usec' => 0))
         ->setLogger($logger);

var_dump("try publish 10 times");

for ($i=0; $i < 10; $i++) {
    try {
        $ret = $producer->publish('app2_t2', 'hello' . "\t" . date('Y-m-d H:i:s'));
        var_dump($ret);
    } catch (Exception $e) {
        var_dump($e->getMessage());
    }
}

sleep(61);
var_dump("try multiPublish after sleep 61s");

$messages = array();
for ($i=0; $i < 10; $i++) { 
    $messages[] = 'world' . "\t" . date('Y-m-d H:i:s');
}

try {
    $ret = $producer->multiPublish('app2_t2', $messages);
    var_dump($ret);
} catch (Exception $e) {
    var_dump($e->getMessage());
}