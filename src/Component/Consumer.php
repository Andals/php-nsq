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

class Consumer
{
    const HEARTBEAT_CNT_TO_RELOOKUP_PER_NSQD = 10;

    /**
     * @var MessageHandler
     */
    private $handler = null;
    private $gunzip  = false;

    /**
     * @var Nsqlookupd[]
     */
    private $nsqlookupds = array();

    /**
     * @var Nsqd[]
     */
    private $nsqds         = array();
    private $nsqdAddresses = array();

    private $topic   = '';
    private $channel = '';

    private $heartbeatCnt           = 0;
    private $heartbeatCntToRelookup = 0;

    /**
     * @var LoggerInterface
     */
    private $logger = null;

    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;

        return $this;
    }

    public function setHandler(MessageHandler $handler)
    {
        $this->handler = $handler;

        return $this;
    }

    public function addNsqlookupd($address)
    {
        $this->nsqlookupds[] = new Nsqlookupd($address);

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

        while (true) {
            foreach ($this->getReadableNsqds() as $nsqd) {
                if (($frame = $this->readFrame($nsqd)) !== false) {
                    $this->processFrame($frame, $nsqd);
                }
            }
        }
    }


    private function lookup()
    {
        $key        = array_rand($this->nsqlookupds);
        $nsqlookupd = $this->nsqlookupds[$key];

        return $nsqlookupd->lookup($this->topic);
    }

    private function relookup()
    {
        $data = $this->lookup();
        if (empty($data)) {
            return false;
        }

        $addresses = array();
        foreach ($data as $item) {
            $address             = $this->makeNsqdAddress($item['host'], $item['tcp_port']);
            $addresses[$address] = 1;
            if (!isset($this->nsqdAddresses[$address])) {
                $this->initNsqd($item);
            }
        }

        foreach ($this->nsqdAddresses as $address => $sid) {
            if (!isset($addresses[$address])) {
                $this->removeNsqd($this->nsqds[$sid]);
            }
        }

        return true;
    }

    private function initNsqds()
    {
        $data = $this->lookup();
        if (empty($data)) {
            throw new \Exception('lookup error');
        }

        foreach ($data as $item) {
            $client = new TcpClient($item['host'], $item['tcp_port']);
            if ($this->initNsqd($client) === false) {
                $client->close();
            }
        }

        $this->heartbeatCntToRelookup = self::HEARTBEAT_CNT_TO_RELOOKUP_PER_NSQD * count($this->nsqds);
    }

    private function initNsqd(TcpClient $client)
    {
        $nsqd = new Nsqd($client);
        if ($nsqd->connect(1) === false) {
            $this->logger->error('nsqd connect error: ' . $nsqd->getTcpClient()->getLastError());
            return false;
        }

        try {
            $nsqd->sendMagic();
            $nsqd->subscribe($this->topic, $this->channel);
            $nsqd->updateRdy(1);
        } catch (\Exception $e) {
            $this->logger->error('nsqd init error: ' . $client->getLastError());
            return false;
        }

        $sid      = $nsqd->getTcpClient()->getSocketId();
        $peerInfo = $client->getPeerInfo();
        $address  = $this->makeNsqdAddress($peerInfo['host'], $peerInfo['port']);

        $this->nsqds[$sid]             = $nsqd;
        $this->nsqdAddresses[$address] = $sid;

        return true;
    }

    private function makeNsqdAddress($host, $port)
    {
        return $host . ':' . $port;
    }

    private function removeNsqd(Nsqd $nsqd)
    {
        $client   = $nsqd->getTcpClient();
        $sid      = $client->getSocketId();
        $peerInfo = $client->getPeerInfo();
        $address  = $this->makeNsqdAddress($peerInfo['host'], $peerInfo['port']);

        $client->close();
        unset($this->nsqds[$sid]);
        unset($this->nsqdAddresses[$address]);
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

    /**
     * @param Error $error
     */
    private function processErrorFrame($error)
    {
        $this->logger->error("recv error frame: " . $error->getMsg());
    }
}