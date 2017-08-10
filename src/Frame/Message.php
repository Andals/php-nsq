<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 15:34
 */

namespace PhpNsq\Frame;

class Message extends Base
{
    private $t = 0;

    public function getFrameType()
    {
        // TODO: Implement getFrameType() method.
        return self::FRAME_TYPE_MESSAGE;
    }

    protected function parsePayload(&$data)
    {
        // TODO: Implement parsePayload() method.
        $t = unpack('J', substr($data, 4, 8));
        $t = current($t);
        $t = substr($t, 0, strlen($t) - 9);
        $t = (int)$t;
    }


    private function parseTimestamp(&$data)
    {

    }
}