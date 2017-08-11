<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 15:29
 */

namespace PhpNsq\Frame;

class Response extends Base
{
    const RESPONSE_OK = 'OK';

    private $contents = '';

    public function getFrameType()
    {
        // TODO: Implement getFrameType() method.
        return self::FRAME_TYPE_RESPONSE;
    }

    public function getContents()
    {
        return $this->contents;
    }

    protected function parsePayload(&$payload)
    {
        // TODO: Implement parsePayload() method.
        $bstr = substr($payload, self::FRAME_SIZE_PAYLOAD);
        $this->contents = $this->parseString($this->payloadSize - self::FRAME_SIZE_FRAME_TYPE, $bstr);
    }
}