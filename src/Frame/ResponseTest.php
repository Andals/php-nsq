<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 15:50
 */

namespace PhpNsq\Frame;

use PHPUnit\Framework\TestCase;

use PhpNsq\Socket\TcpClient;

class ResponseTest extends TestCase
{
    public function clientProvider()
    {
        $client = new TcpClient("127.0.0.1", 4150);
        $client->connect();
        $client->write('  V2');
        $client->write("SUB app2_t2 c1\n");

        return array(
            array($client),
        );
    }

    /**
     * @dataProvider clientProvider
     * @param $client TcpClient
     */
    public function testResponse($client)
    {
        $size = $client->read(Base::FRAME_SIZE_PAYLOAD);
        $size = unpack('N', $size);
        $size = current($size);
        $data = $client->read($size);

        $response = new Response($size, $data);
        $this->assertEquals($response->getContents(), Response::RESPONSE_OK);
    }
}