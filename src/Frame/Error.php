<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 15:54
 */

namespace PhpNsq\Frame;


class Error extends Base
{
    private $msg = '';

    public function getFrameType()
    {
        // TODO: Implement getFrameType() method.
        return self::FRAME_TYPE_ERROR;
    }

    public function getMsg()
    {
        return $this->msg;
    }

    protected function parsePayload(&$payload)
    {
        // TODO: Implement parsePayload() method.
        $bstr      = substr($payload, self::FRAME_SIZE_PAYLOAD);
        $this->msg = $this->parseString($this->payloadSize - self::FRAME_SIZE_FRAME_TYPE, $bstr);
    }
}