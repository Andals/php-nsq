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
    private $socket = null;

    public function __construct($address, $port, $domain = AF_INET)
    {
        $socket = socket_create($domain, SOCK_STREAM, SOL_TCP);
        if ($socket === false) {
            $error = $this->getLastError();
            throw new \Exception("socket_create error: " . $error['msg']);
        }
        $this->socket = $socket;

        if (socket_connect($this->socket, $address, $port) === false) {
            $error = $this->getLastError();
            throw new \Exception("socket_connect error: " . $error['msg']);
        }
    }

    public function getLastError()
    {
        $errno = is_null($this->socket) ? socket_last_error() : socket_last_error($this->socket);

        return array(
            'errno' => $errno,
            'msg'   => socket_strerror($errno),
        );
    }

    /**
     * @param $buf string
     * @return int
     */
    public function write($buf)
    {
        $len   = strlen($buf);
        $total = socket_write($this->socket, $buf, $len);
        if ($total === false) {
            return false;
        }

        while ($total < $len) {
            $n = socket_write($this->socket, substr($buf, $total));
            if ($n === false) {
                return false;
            }
            $total += $n;
        }
    }

    public function read($len, $waitAll = true)
    {
        $buf = socket_read($this->socket, $len);
        if ($buf === false) {
            return false;
        }

        if (!$waitAll) {
            return $buf;
        }

        $total = strlen($buf);
        while ($total < $len) {
            $str = socket_read($this->socket, $len - $total);
            if ($str === false) {
                return false;
            }
            $buf   .= $str;
            $total += strlen($str);
        }

        return $buf;
    }
}