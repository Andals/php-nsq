<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 11:29
 */

namespace PhpNsq\Component;

use PhpNsq\Socket\TcpClient;

class Consumer
{
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
    private $nsqds = array();

    /**
     * @var TcpClient[]
     */
    private $clients = array();

    private $topic   = '';
    private $channel = '';

    public function setHandler(MessageHandler $handler)
    {
        $this->handler = $handler;
    }

    public function addNsqlookupd($address)
    {
        $this->nsqlookupds[] = new Nsqlookupd($address);
    }

    public function messageNeedGunzip($gunzip = true)
    {
        $this->gunzip = $gunzip;
    }

    public function run($topic, $channel)
    {
        $this->topic   = $topic;
        $this->channel = $channel;

        $this->initNsqds();
    }

    private function lookup()
    {
        $key        = array_rand($this->nsqlookupds);
        $nsqlookupd = $this->nsqlookupds[$key];

        return $nsqlookupd->lookup($this->topic);
    }

    private function initNsqds()
    {
        $data = $this->lookup();
        if (empty($data)) {
            throw new \Exception('lookup error');
        }

        foreach ($data as $item) {
            $address = $item['host'] . ':' . $item['tcp_port'];
            $client  = new TcpClient($item['host'], $item['tcp_port']);

            $this->nsqds[$address]   = new Nsqd($client);
            $this->clients[$address] = $client;
        }
    }
}