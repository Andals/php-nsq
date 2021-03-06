<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 11:43
 */

namespace PhpNsq\Component;

use PhpNsq\Frame\Base;
use PhpNsq\Frame\Message;
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

    public function connect($retry = 0)
    {
        return $this->client->connect();
    }

    public function reconnect($retry = 0)
    {
        return $this->client->reconnect();
    }

    public function getTcpClient()
    {
        return $this->client;
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

        $response = $this->readResponse();

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

    public function identify(array $data)
    {
        $data = json_encode($data);
        $data = pack('a' . strlen($data), $data);
        $size = pack('N', strlen($data));

        if ($this->client->write("IDENTIFY\n$size$data") === false) {
            $this->throwClientError('identify');
        }

        $response = $this->readResponse();
        if ($response->getContents() !== Response::RESPONSE_OK) {
            $this->throwClientError('identify');
        }
    }

    public function sendNop()
    {
        if ($this->client->write("NOP\n") === false) {
            $this->throwClientError('nop');
        }
    }

    public function finMessage(Message $message)
    {
        $messageId = $message->getMessageId();
        if ($this->client->write("FIN $messageId\n") === false) {
            $this->throwClientError('fin');
        }
    }

    public function reqMessage(Message $message, $timeout = 0)
    {
        $messageId = $message->getMessageId();
        if ($this->client->write("REQ $messageId $timeout\n") === false) {
            $this->throwClientError('fin');
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

    public function readPayload($payloadSize = 0)
    {
        if ($payloadSize === 0) {
            $payloadSize = $this->readPayloadSize();
        }
        if (($payload = $this->client->read($payloadSize)) === false) {
            $this->throwClientError('read payload');
        }

        return $payload;
    }

    public function publish($topic, $message)
    {
        $size = pack('N', strlen($message));
        $cmd = "PUB $topic\n" . $size . $message;
        if ($this->client->write($cmd) === false) {
            $this->throwClientError('publish');
        }

        $this->checkResponseOK('publish response');
    }

    public function multiPublish($topic, array $messages)
    {
        $bodySize = 4;
        $num = pack('N', count($messages));

        $messageBody = "";
        foreach ($messages as $message) {
            $size = strlen($message);
            $messageBody .= pack('N', $size) . $message;
            $bodySize += $size + 4;
        }

        $cmd = "MPUB $topic\n" . pack('N', $bodySize) . $num . $messageBody;
        if ($this->client->write($cmd) === false) {
            $this->throwClientError('multi publish');
        }

        $this->checkResponseOK('multi publish response');
    }

    private function checkResponseOK($msg)
    {
        $response = $this->readResponse();
        $responseOk = false;
        if ($response->getContents() == Response::RESPONSE_HEARTBEAT) {
            $this->sendNop();

            $response = $this->readResponse();
            if ($response->getContents() == Response::RESPONSE_OK) {
                $responseOk = true;
            }
        } else if ($response->getContents() == Response::RESPONSE_OK) {
            $responseOk = true;
        }

        if (!$responseOk) {
            $this->throwClientError($msg);
        }
    }

    private function readResponse()
    {
        $payloadSize = $this->readPayloadSize();
        $payload     = $this->readPayload($payloadSize);
        return new Response($payloadSize, $payload);
    }

    private function throwClientError($prefix)
    {
        $error = $this->client->getLastError();

        throw new \Exception("$prefix error: " . $error['msg']);
    }
}