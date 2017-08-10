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
    const FRAME_SIZE_TIMESTAMP  = 8;
    const FRAME_SIZE_ATTEMPTS   = 2;
    const FRAME_SIZE_MESSAGE_ID = 16;

    private $enableGzip = false;

    private $t           = 0;
    private $attempts    = 0;
    private $messageId   = '';
    private $messageBody = '';

    public function __construct($payloadSize, $data, $enableGzip = true)
    {
        $this->enableGzip = $enableGzip;

        parent::__construct($payloadSize, $data);
    }

    public function getFrameType()
    {
        // TODO: Implement getFrameType() method.
        return self::FRAME_TYPE_MESSAGE;
    }

    public function getTimestamp()
    {
        return $this->t;
    }

    public function getAttempts()
    {
        return $this->attempts;
    }

    public function getMessageId()
    {
        return $this->messageId;
    }

    public function getMessageBody()
    {
        return $this->messageBody;
    }

    protected function parsePayload(&$data)
    {
        // TODO: Implement parsePayload() method.
        $this->parseTimestamp($data);
        $this->parseAttempts($data);
        $this->parseMessageId($data);
        $this->parseMessageBody($data);
    }


    private function parseTimestamp(&$data)
    {
        $t = unpack('J', substr($data, self::FRAME_SIZE_FRAME_TYPE, self::FRAME_SIZE_TIMESTAMP));
        $t = current($t);

        //convert nanoseconds to seconds
        $t       = substr($t, 0, strlen($t) - 9);
        $this->t = (int)$t;
    }

    private function parseAttempts(&$data)
    {
        $start    = self::FRAME_SIZE_FRAME_TYPE + self::FRAME_SIZE_TIMESTAMP;
        $attempts = unpack('n', substr($data, $start, self::FRAME_SIZE_ATTEMPTS));

        $this->attempts = current($attempts);
    }

    private function parseMessageId(&$data)
    {
        $start = self::FRAME_SIZE_FRAME_TYPE + self::FRAME_SIZE_TIMESTAMP + self::FRAME_SIZE_ATTEMPTS;
        $bdata = substr($data, $start, self::FRAME_SIZE_MESSAGE_ID);

        $this->messageId = $this->parseString(self::FRAME_SIZE_MESSAGE_ID, $bdata);
    }

    private function parseMessageBody(&$data)
    {
        $start = self::FRAME_SIZE_FRAME_TYPE + self::FRAME_SIZE_TIMESTAMP + self::FRAME_SIZE_ATTEMPTS + self::FRAME_SIZE_MESSAGE_ID;
        $bdata = substr($data, $start);

        $this->messageBody = $this->parseString($this->payloadSize - $start, $bdata);
        if ($this->enableGzip) {
            $this->messageBody = gzdecode($this->messageBody);
        }
    }
}