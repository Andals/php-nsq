<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/10
 * Time: 18:07
 */

namespace PhpNsq\Component;

use PHPUnit\Framework\TestCase;

class NsqlookupdTest extends TestCase
{
    public function lookupdProvider()
    {
        $nsqlookupd = new Nsqlookupd("http://127.0.0.1:4161");

        return array(
            array($nsqlookupd),
        );
    }

    /**
     * @dataProvider lookupdProvider
     * @param $nsqlookupd Nsqlookupd
     */
    public function testClient($nsqlookupd)
    {
        $data = $nsqlookupd->lookup('app2_t2');
        $this->assertEquals($data[0]['host'], '127.0.0.1');
        $this->assertEquals($data[0]['tcp_port'], 4150);
    }
}