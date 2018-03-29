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
         ->setLogger($logger);

// for ($i=0; $i < 10; $i++) { 
//     $producer->publish('app2_t2', 'hello' . "\t" . date('Y-m-d H:i:s'));
// }
// exit;

$messages = array();
for ($i=0; $i < 10; $i++) { 
    $messages[] = 'world' . "\t" . date('Y-m-d H:i:s');
}
$producer->multiPublish('app2_t2', $messages);