<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 11:43
 */

namespace PhpNsq\Component;

use PhpNsq\Frame\Base;
use PhpNsq\Socket\TcpClient;
use PhpNsq\Frame\Response;

class Nsqd
{
    /**
     * @var TcpClient
     */
    private $client = null;

    public function __construct(TcpClient $client)
    {
        $this->client = $client;
    }

    public function getTcpClientSocket()
    {
        return $this->client->getSocket();
    }

    public function sendMagic()
    {
        if ($this->client->write('  V2') === false) {
            $this->throwClientError('send magic');
        }
    }

    public function subscribe($topic, $channel)
    {
        if ($this->client->write("SUB $topic $channel\n") === false) {
            $this->throwClientError('subscribe');
        }

        $payloadSize = $this->readPayloadSize();
        $payload     = $this->readPayload($payloadSize);
        $response    = new Response($payloadSize, $payload);
        if ($response->getContents() !== Response::RESPONSE_OK) {
            $this->throwClientError('subscribe');
        }
    }

    public function updateRdy($cnt)
    {
        if ($this->client->write("RDY $cnt\n") === false) {
            $this->throwClientError('update rdy');
        }
    }

    public function readPayloadSize()
    {
        if (($size = $this->client->read(Base::FRAME_SIZE_PAYLOAD)) === false) {
            $this->throwClientError('read size');
        }

        $size = unpack('N', $size);
        $size = current($size);
        if (!is_numeric($size)) {
            $this->throwClientError('read size');
        }

        return $size;
    }

    public function readPayload($payloadSize)
    {
        if (($payload = $this->client->read($payloadSize)) === false) {
            $this->throwClientError('read payload');
        }

        return $payload;
    }

    private function throwClientError($prefix)
    {
        $error = $this->client->getLastError();

        throw new \Exception("$prefix error: " . $error['msg']);
    }
}