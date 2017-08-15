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
            $address             = $item['host'] . ':' . $item['tcp_port'];
            $addresses[$address] = 1;
            if (!isset($this->nsqdAddresses[$address])) {
                $this->initNsqd($item);
            }
        }

        foreach ($this->nsqdAddresses as $address => $sid) {
            if (!isset($addresses[$address])) {
                $nsqd = $this->nsqds[$sid];
                $nsqd->getTcpClient()->close();
                unset($this->nsqds[$sid]);
                unset($this->nsqdAddresses[$address]);
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
            $this->initNsqd($item);
        }

        $this->heartbeatCntToRelookup = self::HEARTBEAT_CNT_TO_RELOOKUP_PER_NSQD * count($this->nsqds);
    }

    private function initNsqd(array $item)
    {
        $client = new TcpClient($item['host'], $item['tcp_port']);
        $nsqd   = new Nsqd($client);

        $nsqd->sendMagic();
        $nsqd->subscribe($this->topic, $this->channel);
        $nsqd->updateRdy(1);

        $sid     = $nsqd->getTcpClient()->getSocketId();
        $address = $item['host'] . ':' . $item['tcp_port'];

        $this->nsqds[$sid]             = $nsqd;
        $this->nsqdAddresses[$address] = $sid;
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

        if (($r = socket_select($read, $write, $except, null)) === false) {
            throw new \Exception("socket select error: " . socket_strerror(socket_last_error()));
        }

        $nsqds = array();
        foreach ($read as $socket) {
            $sid = intval($socket);
            if (!isset($this->nsqds[$sid])) {
                throw new \Exception("socket select error: invalid socket id");
            }
            $nsqds[] = $this->nsqds[$sid];
        }

        return $nsqds;
    }

    /**
     * @param Nsqd $nsqd
     * @return bool|Error|Message|Response
     */
    private function readFrame(Nsqd $nsqd)
    {
        $payloadSize = $nsqd->readPayloadSize();
        $payload     = $nsqd->readPayload($payloadSize);

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
            $this->logger->info("recv response $contents");
        }
    }

    /**
     * @param Error $error
     */
    private function processErrorFrame($error)
    {
        $this->logger->error("recv error " . $error->getMsg());
    }
}