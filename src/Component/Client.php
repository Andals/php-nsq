<?php
/**
 * Created by IntelliJ IDEA.
 * User: tabalt
 * Date: 2017/9/14
 * Time: 15:20
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

abstract class Client
{
    const HEARTBEAT_CNT_TO_RELOOKUP_PER_NSQD = 10;

    const SOCKET_TIMEOUT_S  = 30;
    const SOCKET_TIMEOUT_MS = 0;

    private $timeout = array(
        'sec' => self::SOCKET_TIMEOUT_S,
        'usec' => self::SOCKET_TIMEOUT_MS,
    );

    /**
     * @var Nsqlookupd[]
     */
    protected $nsqlookupds = array();

    /**
     * @var Nsqd[]
     */
    protected $nsqds         = array();
    protected $nsqdAddresses = array();
    protected $nsqdsInitialized = false;

    protected $topic   = '';

    protected $heartbeatCnt           = 0;
    protected $heartbeatCntToRelookup = 0;

    /**
     * @var LoggerInterface
     */
    protected $logger = null;

    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    public function setTimeout(array $timeout)
    {
        $this->timeout = array_merge($this->timeout, $timeout);

        return $this;
    }

    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;

        return $this;
    }

    public function addNsqlookupd($address)
    {
        $this->nsqlookupds[] = new Nsqlookupd($address);

        return $this;
    }

    protected function updateHeartbeatCntAndTryRelookup()
    {
        $this->heartbeatCnt++;
        if ($this->heartbeatCnt === $this->heartbeatCntToRelookup) {
            if ($this->relookup() === false) {
                $this->logger->error("lookup error");
                $this->heartbeatCnt--;
            } else {
                $this->heartbeatCnt = 0;
            }
        }
    }

    protected function lookup()
    {
        $key        = array_rand($this->nsqlookupds);
        $nsqlookupd = $this->nsqlookupds[$key];

        return $nsqlookupd->lookup($this->topic);
    }

    protected function relookup()
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

        $this->updateHeartbeatCntToRelookup();
        return true;
    }

    protected function initNsqds()
    {
        if ($this->nsqdsInitialized) {
            return;
        }

        $data = $this->lookup();
        if (empty($data)) {
            throw new \Exception('lookup error');
        }

        foreach ($data as $item) {
            $client = new TcpClient($item['host'], $item['tcp_port'], AF_INET, $this->timeout);
            if ($this->initNsqd($client) === false) {
                $client->close();
            }
        }

        $this->updateHeartbeatCntToRelookup();
        $this->nsqdsInitialized = true;
    }

    protected function initNsqd(TcpClient $client)
    {
        $nsqd = new Nsqd($client);
        if ($nsqd->connect(1) === false) {
            $this->logger->error('nsqd connect error: ' . $nsqd->getTcpClient()->getLastError());
            return false;
        }

        try {
            $nsqd->sendMagic();
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

    protected function removeNsqd(Nsqd $nsqd)
    {
        $client   = $nsqd->getTcpClient();
        $sid      = $client->getSocketId();
        $peerInfo = $client->getPeerInfo();
        $address  = $this->makeNsqdAddress($peerInfo['host'], $peerInfo['port']);

        $client->close();
        unset($this->nsqds[$sid]);
        unset($this->nsqdAddresses[$address]);

        $this->updateHeartbeatCntToRelookup();
    }

    protected function updateHeartbeatCntToRelookup()
    {
        $this->heartbeatCntToRelookup = self::HEARTBEAT_CNT_TO_RELOOKUP_PER_NSQD * count($this->nsqds);
    }

    protected function makeNsqdAddress($host, $port)
    {
        return $host . ':' . $port;
    }
}