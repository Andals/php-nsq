<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 16:00
 */

namespace PhpNsq\Frame;

use PHPUnit\Framework\TestCase;

use PhpNsq\Socket\TcpClient;

class ErrorTest extends TestCase
{
    public function clientProvider()
    {
        $client = new TcpClient("127.0.0.1", 4150);
        $client->write('  V2');
        $client->write("FIN xxxyyy\n");

        return array(
            array($client),
        );
    }

    /**
     * @dataProvider clientProvider
     * @param $client TcpClient
     */
    public function testError($client)
    {
        $size = $client->read(Base::FRAME_SIZE_PAYLOAD);
        $size = unpack('N', $size);
        $size = current($size);
        $data = $client->read($size);

        $error = new Error($size, $data);
        var_dump($error->getMsg());
    }
}