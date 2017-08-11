<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 15:04
 */

namespace PhpNsq\Component;

use PHPUnit\Framework\TestCase;

use PhpNsq\Frame\Message;

class NsqdTest extends TestCase
{
    public function nsqdProvider()
    {
        $nsqd = new Nsqd("127.0.0.1", 4150);
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
}