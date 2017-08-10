<?php

/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 14:53
 */

namespace PhpNsq\Frame;

abstract class Base
{
    const FRAME_TYPE_RESPONSE = 0;
    const FRAME_TYPE_ERROR    = 1;
    const FRAME_TYPE_MESSAGE  = 2;

    const FRAME_SIZE_PAYLOAD = 4;
    const FRAME_SIZE_FRAME_TYPE = 4;

    protected $payloadSize = 0;

    abstract public function getFrameType();

    abstract protected function parsePayload(&$data);

    public function __construct($payloadSize, &$data)
    {
        $this->payloadSize = $payloadSize;
        $this->parsePayload($data);
    }

    public function getPayloadSize()
    {
        return $this->payloadSize;
    }


    protected function parseString($len, &$bstr)
    {
        $chars  = unpack('c' . $len, $bstr);
        $result = '';
        foreach ($chars as $char) {
            $result .= chr($char);
        }

        return $result;
    }
}