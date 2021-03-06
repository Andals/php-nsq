<?php

/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/8
 * Time: 18:09
 */

namespace PhpNsq\Socket;

class TcpClient
{
    private $socket    = null;
    private $peerInfo  = array();
    private $connected = false;

    private $timeout = null;

    public function __construct($host, $port, $domain = AF_INET, $timeout = null)
    {
        $this->peerInfo = array(
            'host'   => $host,
            'port'   => $port,
            'domain' => $domain,
        );

        if (!empty($timeout) && isset($timeout['sec']) && isset($timeout['usec'])) {
            $this->timeout = $timeout;
        }

        $this->createSocket();
    }

    private function createSocket()
    {
        $socket = socket_create($this->peerInfo['domain'], SOCK_STREAM, SOL_TCP);
        if ($socket === false) {
            $error = $this->getLastError();
            throw new \Exception("socket_create error: " . $error['msg']);
        }

        if (!empty($this->timeout)) {
            if (@socket_set_option($socket, SOL_SOCKET, SO_SNDTIMEO, $this->timeout) === false) {
                $error = $this->getLastError();
                throw new \Exception("socket_set_option SO_SNDTIMEO error: " . $error['msg']);
            }

            if (@socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, $this->timeout) === false) {
                $error = $this->getLastError();
                throw new \Exception("socket_set_option SO_RCVTIMEO error: " . $error['msg']);
            }
        }

        $this->socket = $socket;
    }

    public function connect($retry = 0)
    {
        if ($this->connected) {
            return true;
        }

        if (@socket_connect($this->socket, $this->peerInfo['host'], $this->peerInfo['port']) === true) {
            $this->connected = true;
            return true;
        }

        for ($i = 0; $i < $retry; $i++) {
            if (@socket_connect($this->socket, $this->peerInfo['host'], $this->peerInfo['port']) === true) {
                $this->connected = true;
                return true;
            }
        }

        return false;
    }

    public function reconnect($retry = 0)
    {
        $this->close();
        $this->connected = false;
        $this->socket    = null;

        $this->createSocket();
        return $this->connect($retry);
    }

    public function getLastError()
    {
        $errno = is_null($this->socket) ? socket_last_error() : socket_last_error($this->socket);

        return array(
            'errno' => $errno,
            'msg'   => socket_strerror($errno),
        );
    }

    public function getPeerInfo()
    {
        return $this->peerInfo;
    }

    public function getSocket()
    {
        return $this->socket;
    }

    public function getSocketId()
    {
        return intval($this->socket);
    }

    public function close()
    {
        if (!is_null($this->socket)) {
            socket_close($this->socket);
            $this->socket = null;
        }
    }

    /**
     * @param $buf string
     * @return int
     */
    public function write($buf)
    {
        $len   = strlen($buf);
        $total = @socket_write($this->socket, $buf, $len);
        if ($total === false) {
            return false;
        }

        while ($total < $len) {
            $n = @socket_write($this->socket, substr($buf, $total));
            if ($n === false) {
                return false;
            }
            $total += $n;
        }

        return $len;
    }

    public function read($len, $waitAll = true)
    {
        $buf = @socket_read($this->socket, $len);
        if ($buf === false) {
            return false;
        }

        if (!$waitAll) {
            return $buf;
        }

        $total = strlen($buf);
        while ($total < $len) {
            $str = @socket_read($this->socket, $len - $total);
            if ($str === false) {
                return false;
            }
            $buf   .= $str;
            $total += strlen($str);
        }

        return $buf;
    }
}