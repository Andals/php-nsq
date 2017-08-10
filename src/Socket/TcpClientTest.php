<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/9
 * Time: 10:38
 */

namespace PhpNsq\Socket;

use PHPUnit\Framework\TestCase;

class TcpClientTest extends TestCase
{
    public function clientProvider()
    {
        $client = new TcpClient("127.0.0.1", 4150);

        return array(
            array($client),
        );
    }

    /**
     * @dataProvider clientProvider
     * @param $client TcpClient
     */
    public function testClient($client)
    {
        $client->write('  V2');
        $client->write("SUB app2_t2 c1\n");
        $bsize = $client->read(4);
        $asize = unpack('N', $bsize);
        $msgSize = current($asize);

        $bdata = $client->read($msgSize);
        $frame = unpack('N', substr($bdata, 0, 4));

        $chars    = unpack('c2', substr($bdata, 4));
        $response = '';
        foreach ($chars as $char) {
            $response .= chr($char);
        }
        var_dump($response);

        $client->write("RDY 1\n");
        $msgSize = $client->read(4);
        $asize = unpack('N', $msgSize);
        $msgSize = current($asize);
        $bdata = $client->read($msgSize);
        $frame = unpack('N', substr($bdata, 0, 4));
        $t = unpack('J', substr($bdata, 4, 8));
        $t = current($t);
        $t = substr($t, 0, strlen($t) - 9);
        $t = (int)$t;

        $attempts = unpack('n', substr($bdata, 12, 2));
        $attempts = current($attempts);

        $chars = unpack('c16', substr($bdata, 14, 16));
        $msgId = '';
        foreach ($chars as $char) {
            $msgId .= chr($char);
        }

        $bodySize = $msgSize - 30;
        $chars = unpack('c'.$bodySize, substr($bdata, 30));
        $body = '';
        foreach ($chars as $char) {
            $body .= chr($char);
        }
        var_dump(gzdecode($body));
    }
}