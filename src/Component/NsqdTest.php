<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 15:04
 */

namespace PhpNsq\Component;

use PhpNsq\Frame\Response;
use PhpNsq\Frame\Tool;
use PhpNsq\Socket\TcpClient;
use PHPUnit\Framework\TestCase;

use PhpNsq\Frame\Message;

class NsqdTest extends TestCase
{
    public function nsqdProvider()
    {
        $nsqd = new Nsqd(new TcpClient('127.0.0.1', 4150));
        $nsqd->connect();
        $nsqd->sendMagic();

        return array(
            array($nsqd),
        );
    }

    /**
     * @dataProvider nsqdProvider
     * @param $nsqd Nsqd
     */
    public function testReadPayload($nsqd)
    {
        $nsqd->subscribe('app2_t2', 'c1');
        $nsqd->updateRdy(1);

        $size = $nsqd->readPayloadSize();
        var_dump($size);
        $payload = $nsqd->readPayload($size);

        $message = new Message($size, $payload);
        var_dump($message->getMessageBody());
    }

    /**
     * @dataProvider nsqdProvider
     * @param $nsqd Nsqd
     */
    public function testIdentify($nsqd)
    {
        $data = array(
            'hostname'           => $_SERVER['HOSTNAME'],
            'heartbeat_interval' => 3000,
        );
        $nsqd->identify($data);

        $payloadSize = $nsqd->readPayloadSize();
        $payload = $nsqd->readPayload($payloadSize);

        $frameType = Tool::parseFrameType($payload);
        echo "$frameType\n";
        $response = new Response($payloadSize, $payload);
        var_dump($response->getContents());
    }
}