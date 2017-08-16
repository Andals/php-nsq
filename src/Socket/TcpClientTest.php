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
        $client->connect();

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

    public function testSelect()
    {
        $client1 = new TcpClient("127.0.0.1", 4150);
        $client2 = new TcpClient("10.16.59.187", 4150);
        $client1->connect();
        $client2->connect();
        $socket1 = $client1->getSocket();
        $socket2 = $client2->getSocket();
        $allRead = array($socket1, $socket2);
        $write   = $except = array();
        var_dump('allRead', $allRead);

        foreach (array($client1, $client2) as $client) {
            $client->write('  V2');
            $client->write("SUB app2_t2 c1\n");
        }

        $client1->write("RDY 1\n");

        $read = $allRead;
        $r    = socket_select($read, $write, $except, null);
        var_dump($r, intval($read[0]));

        $client1->read(10);
        $msgSize = $client1->read(4);
        $asize   = unpack('N', $msgSize);
        $msgSize = current($asize);
        $client1->read($msgSize);

        $read = $allRead;
        $r    = socket_select($read, $write, $except, null);
        var_dump($r, $read);
    }
}