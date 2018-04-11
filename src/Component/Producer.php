<?php
/**
 * Created by IntelliJ IDEA.
 * User: tabalt
 * Date: 2017/9/14
 * Time: 15:20
 */

namespace PhpNsq\Component;

use PhpNsq\Frame\Base;
use PhpNsq\Frame\Error;
use PhpNsq\Frame\Message;
use PhpNsq\Frame\Response;
use PhpNsq\Frame\Tool;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Producer extends Client
{
    private $gzip  = false;

    public function messageNeedGzip($gzip = true)
    {
        $this->gzip = $gzip;

        return $this;
    }

    public function publish($topic, $message)
    {
        $this->topic = $topic;
        if ($this->gzip) {
            $message = gzencode($message);
        }

        $this->initNsqds();

        $success = false;
        while (true) {
            $randKey = array_rand($this->nsqds);
            $nsqd = isset($this->nsqds[$randKey]) ? $this->nsqds[$randKey] : array();
            if (empty($nsqd)) {
                $this->logger->error("empty nsqd error at publish");
                break;
            }

            try{
                $nsqd->publish($topic, $message);
                $this->updateHeartbeatCntAndTryRelookup();
                $success = true;
                break;
            } catch (\Exception $e) {
                $this->logger->error("publish error: " . $e->getMessage());
                if ($nsqd->reconnect()) {
                    $nsqd->sendMagic();
                } else {
                    $this->removeNsqd($nsqd);
                }
            }
        }

        return $success;
    }

    public function multiPublish($topic, Array $messages)
    {
        $this->topic = $topic;
        if ($this->gzip) {
            foreach ($messages as $key => $message) {
                $messages[$key] = gzencode($message);
            }
        }

        $this->initNsqds();

        $success = false;
        while (true) {
            $randKey = array_rand($this->nsqds);
            $nsqd = isset($this->nsqds[$randKey]) ? $this->nsqds[$randKey] : array();
            if (empty($nsqd)) {
                $this->logger->error("empty nsqd error at multiPublish");
                break;
            }

            try{
                $nsqd->multiPublish($topic, $messages);
                $this->updateHeartbeatCntAndTryRelookup();
                $success = true;
                break;
            } catch (\Exception $e) {
                $this->logger->error("multiPublish error: " . $e->getMessage());
                if ($nsqd->reconnect()) {
                    $nsqd->sendMagic();
                } else {
                    $this->removeNsqd($nsqd);
                }
            }
        }

        return $success;
    }
}