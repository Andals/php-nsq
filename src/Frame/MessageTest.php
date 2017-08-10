<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 16:52
 */

namespace PhpNsq\Frame;

use PHPUnit\Framework\TestCase;

use PhpNsq\Socket\TcpClient;

class MessageTest extends TestCase
{
    public function clientProvider()
    {
        $client = new TcpClient("127.0.0.1", 4150);
        $client->write('  V2');
        $client->write("SUB app2_t2 c1\n");

        $client->read(Base::FRAME_SIZE_PAYLOAD + Base::FRAME_SIZE_FRAME_TYPE + strlen(Response::RESPONSE_OK));
        $client->write("RDY 1\n");

        return array(
            array($client),
        );
    }

    /**
     * @dataProvider clientProvider
     * @param $client TcpClient
     */
    public function testMessage($client)
    {
        $size = $client->read(Base::FRAME_SIZE_PAYLOAD);
        $size = unpack('N', $size);
        $size = current($size);
        $data = $client->read($size);

        $message = new Message($size, $data);
        var_dump($message->getTimestamp(), $message->getAttempts(), $message->getMessageId(), $message->getMessageBody());
    }
}