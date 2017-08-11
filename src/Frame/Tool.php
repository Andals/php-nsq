<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 15:47
 */

namespace PhpNsq\Frame;


class Tool
{
    public static function parseFrameType(&$payload)
    {
        $type = substr($payload, 0, Base::FRAME_SIZE_FRAME_TYPE);
        $type = unpack('N', $type);

        return current($type);
    }
}