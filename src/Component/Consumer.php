<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 11:29
 */

namespace PhpNsq\Component;

use PhpNsq\Frame\Base;
use PhpNsq\Frame\Error;
use PhpNsq\Frame\Message;
use PhpNsq\Frame\Response;
use PhpNsq\Frame\Tool;
use PhpNsq\Socket\TcpClient;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Consumer extends ClientBase
{
    /**
     * @var MessageHandler
     */
    private $handler = null;
    private $gunzip  = false;

    public function setHandler(MessageHandler $handler)
    {
        $this->handler = $handler;

        return $this;
    }

    public function messageNeedGunzip($gunzip = true)
    {
        $this->gunzip = $gunzip;

        return $this;
    }

    public function run($topic, $channel)
    {
        $this->topic   = $topic;
        $this->channel = $channel;

        $this->initNsqds();
        $this->subscribeNsqds();

        while (true) {
            foreach ($this->getReadableNsqds() as $nsqd) {
                if (($frame = $this->readFrame($nsqd)) !== false) {
                    $this->processFrame($frame, $nsqd);
                }
            }
        }
    }

    private function subscribeNsqds()
    {
        foreach ($this->nsqds as $nsqd) {
            $nsqd->subscribe($this->topic, $this->channel);
            $nsqd->updateRdy(1);
        }
    }

    /**
     * @return Nsqd[]
     * @throws \Exception
     */
    private function getReadableNsqds()
    {
        $write = $except = array();
        $read  = array();
        foreach ($this->nsqds as $nsqd) {
            $read[] = $nsqd->getTcpClient()->getSocket();
        }

        if (($r = @socket_select($read, $write, $except, null)) === false) {
            $this->logger->error('socket select error: ' . socket_strerror(socket_last_error()));
            return array();
        }

        $nsqds = array();
        foreach ($read as $socket) {
            $sid = intval($socket);
            if (!isset($this->nsqds[$sid])) {
                $this->logger->error("socket select error: invalid socket id");
            } else {
                $nsqds[] = $this->nsqds[$sid];
            }
        }

        return $nsqds;
    }

    /**
     * @param Nsqd $nsqd
     * @return bool|Error|Message|Response
     */
    private function readFrame(Nsqd $nsqd)
    {
        $payloadSize = 0;
        $payload     = '';
        try {
            $payloadSize = $nsqd->readPayloadSize();
            $payload     = $nsqd->readPayload($payloadSize);
        } catch (\Exception $e) {
            $this->logger->error("read payloadSize or payload error: " . $e->getMessage());

            $client = $nsqd->getTcpClient();
            $this->removeNsqd($nsqd);
            if ($client->reconnect(1) === false) {
                $this->logger->error('nsqd reconnect error: ' . $nsqd->getTcpClient()->getLastError());
                $client->close();
                return false;
            }

            if ($this->initNsqd($client) === false) {
                $this->logger->error('init nsqd after reconnect error');
                $client->close();
                return false;
            }

            try {
                $payloadSize = $nsqd->readPayloadSize();
                $payload     = $nsqd->readPayload($payloadSize);
            } catch (\Exception $e) {
                $this->logger->error('after retry read payloadSize or payload error: ' . $e->getMessage());
                $this->removeNsqd($this->nsqds[$client->getSocketId()]);
                return false;
            }
        }

        switch (Tool::parseFrameType($payload)) {
            case Base::FRAME_TYPE_MESSAGE:
                return new Message($payloadSize, $payload, $this->gunzip);
            case Base::FRAME_TYPE_RESPONSE:
                return new Response($payloadSize, $payload);
            case Base::FRAME_TYPE_ERROR:
                return new Error($payloadSize, $payload);
            default:
                return false;
        }
    }

    /**
     * @param Error|Message|Response $frame
     * @param Nsqd $nsqd
     */
    private function processFrame($frame, Nsqd $nsqd)
    {
        switch ($frame->getFrameType()) {
            case Base::FRAME_TYPE_MESSAGE:
                $this->processMessageFrame($frame, $nsqd);
                break;
            case Base::FRAME_TYPE_RESPONSE:
                $this->processResponseFrame($frame, $nsqd);
                break;
            case Base::FRAME_TYPE_ERROR:
                $this->processErrorFrame($frame);
                break;
        }
    }

    /**
     * @param Message $message
     * @param Nsqd $nsqd
     */
    private function processMessageFrame($message, Nsqd $nsqd)
    {
        $success = $this->handler->handleMessage($message);
        if ($success === true) {
            $nsqd->finMessage($message);
        } else {
            $nsqd->reqMessage($message);
        }
    }

    /**
     * @param Response $response
     * @param Nsqd $nsqd
     */
    private function processResponseFrame($response, Nsqd $nsqd)
    {
        $contents = $response->getContents();
        if ($contents === Response::RESPONSE_HEARTBEAT) {
            $nsqd->sendNop();

            $this->heartbeatCnt++;
            if ($this->heartbeatCnt === $this->heartbeatCntToRelookup) {
                if ($this->relookup() === false) {
                    $this->logger->error("lookup error");
                    $this->heartbeatCnt--;
                } else {
                    $this->heartbeatCnt = 0;
                }
            }
        } else {
            $this->logger->info("recv response frame: $contents");
        }
    }
}