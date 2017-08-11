<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/11
 * Time: 11:29
 */

namespace PhpNsq\Component;

class Consumer
{
    /**
     * @var MessageHandler
     */
    private $handler = null;

    private $nsqlookupds = array();

    public function setHandler(MessageHandler $handler)
    {
        $this->handler = $handler;
    }

    public function addNsqlookupd($address)
    {
        $this->nsqlookupds[] = new Nsqlookupd($address);
    }

    public function run()
    {

    }
}