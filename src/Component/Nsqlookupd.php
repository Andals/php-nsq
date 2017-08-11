<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 17:57
 */

namespace PhpNsq\Component;

use PhpNsq\Misc\Curl;

class Nsqlookupd
{
    private $address = '';
    private $curl    = null;

    /**
     * Nsqlookupd constructor.
     * @param string $address : http://127.0.0.1:4151
     */
    public function __construct($address)
    {
        $this->address = $address;
        $this->curl    = Curl::ins();
    }

    public function lookup($topic)
    {
        $response = $this->curl->get("$this->address/lookup?topic=$topic");
        $data     = json_decode($response, true);
        if (!isset($data['producers'])) {
            return array();
        }

        $result = array();
        foreach ($data['producers'] as $item) {
            $result[] = array(
                'host'     => $item['broadcast_address'],
                'tcp_port' => $item['tcp_port'],
            );
        }

        return $result;
    }
}