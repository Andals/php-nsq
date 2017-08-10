<?php
namespace PhpNsq\Misc;

class Curl
{/*{{{*/
    const TIME_INDEX        = 0;
    const MODE_INDEX        = 1;
    const DEF_TIMEOUT       = 3;
    const SECOND_MODE       = 's';
    const MILLI_SECOND_MODE = 'ms';

    private static $_ins = null;

    private $_ch = null;
    private $sleepTime = 1;

    private function __construct()
    {/*{{{*/
        $this->_ch = curl_init();
        $this->setCommon();
    }/*}}}*/

    public static function ins()
    {/*{{{*/
        if(is_null(self::$_ins))
        {
            self::$_ins = new self();
        }
        return self::$_ins;
    }/*}}}*/

    //timeout若设置级别为毫秒请用array($time,'ms')
    public function get($url, $timeout=self::DEF_TIMEOUT, $ips=array(), $retry=0, $opts=array())
    {/*{{{*/
        $r = $this->prepareRequest($url, $timeout, $ips, $opts);
        if(false === $r)
        {
            return false;
        }

        return $this->getResponse($retry);
    }/*}}}*/
    public function post($url, $postData, $timeout=self::DEF_TIMEOUT, $ips=array(), $retry=0, $opts=array())
    {/*{{{*/
        $r = $this->prepareRequest($url, $timeout, $ips, $opts);
        if(false === $r)
        {
            return false;
        }

        $this->setPostData($postData);
        $response = $this->getResponse($retry);
        $this->clearPostStatus();

        return $response;
    }/*}}}*/
    public function getTotalTime()
    {/*{{{*/
        return curl_getinfo($this->_ch, CURLINFO_TOTAL_TIME);
    }/*}}}*/
    public function getError()
    {/*{{{*/
        return curl_error($this->_ch);
    }/*}}}*/
    public function getHttpCode()
    {/*{{{*/
        return curl_getinfo($this->_ch, CURLINFO_HTTP_CODE);
    }/*}}}*/
	public function setCookie($cookie_data=array())
	{/*{{{*/
		$cookie = array();
		foreach($cookie_data as $key => $value)
		{
			$cookie[] = $key.'='.$value;
		}
		$cookie = implode(';', $cookie);

        curl_setopt($this->_ch, CURLOPT_COOKIE, $cookie);
	}/*}}}*/

    private function setCommon()
    {/*{{{*/
        curl_setopt($this->_ch, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        curl_setopt($this->_ch, CURLOPT_NOPROGRESS, 1);
        curl_setopt($this->_ch, CURLOPT_NOBODY, 0);
        curl_setopt($this->_ch, CURLOPT_ENCODING, '');
        curl_setopt($this->_ch, CURLOPT_COOKIEFILE, 1);
        curl_setopt($this->_ch, CURLOPT_FOLLOWLOCATION, 1);
        curl_setopt($this->_ch, CURLOPT_MAXREDIRS, 3);
        curl_setopt($this->_ch, CURLOPT_USERAGENT, 'Mozilla/4.0 (compatible; MSIE 5.00; Windows 98)');
        curl_setopt($this->_ch, CURLOPT_RETURNTRANSFER, 1);
    }/*}}}*/
    private function prepareRequest($url, $timeout, $ips, $opts)
    {/*{{{*/
        if('' == $url)
        {
            return false;
        }
        $urlAry = parse_url($url);
        if(false === $urlAry || !isset($urlAry['host']))
        {
            return false;
        }
        $host = $urlAry['host'];

        $this->setUrl($url, $host, $ips);
        $this->setTimeout($timeout);
        $this->setHttpHeader($host);
        if(is_array($opts) && !empty($opts))
        {
            $this->setOpts($opts);
        }
        return true;
    }/*}}}*/
    private function setUrl($url, $host, $ips)
    {/*{{{*/
        if(is_array($ips) && !empty($ips))
        {
            $index = array_rand($ips);
            $ip    = $ips[$index];
            $url   = str_replace($host, $ip, $url);
        }
        curl_setopt($this->_ch, CURLOPT_URL, $url);
    }/*}}}*/
    private function setTimeout($timeout)
    {/*{{{*/
        if(is_numeric($timeout))
        {
            $mode  = self::SECOND_MODE;
            $value = $timeout;
        }
        else if(is_array($timeout))
        {
            $mode  = $timeout[self::MODE_INDEX];
            $value = $timeout[self::TIME_INDEX];
        }
        else
        {
            $mode  = self::SECOND_MODE;
            $value = self::DEF_TIMEOUT;
        }

        switch($mode)
        {
            case self::SECOND_MODE:
                curl_setopt($this->_ch, CURLOPT_TIMEOUT, $value);
                break;
            case self::MILLI_SECOND_MODE:
                curl_setopt($this->_ch, CURLOPT_NOSIGNAL, true);
                curl_setopt($this->_ch, CURLOPT_TIMEOUT_MS, $value);
                break;
            default:
                curl_setopt($this->_ch, CURLOPT_TIMEOUT, self::DEF_TIMEOUT);
        }
    }/*}}}*/
    private function setHttpHeader($host)
    {/*{{{*/
        $httpHeader   = array();
        $httpHeader[] = 'Connection: Keep-Alive';
        $httpHeader[] = 'Pragma: no-cache';
        $httpHeader[] = 'Cache-Control: no-cache';
        $httpHeader[] = 'Accept: */*';
        $httpHeader[] = 'Host: '.$host;

        curl_setopt($this->_ch, CURLOPT_HTTPHEADER, $httpHeader);
    }/*}}}*/
    private function setOpts($opts)
    {/*{{{*/
        foreach($opts as $key => $value)
        {
            curl_setopt($this->_ch, $key, $value);
        }
    }/*}}}*/
    private function getHost($urlAry, $hosts)
    {/*{{{*/
        $host = $urlAry['host'];
        if(is_array($hosts) && !empty($hosts))
        {
            $index = array_rand($hosts);
            $host  = $hosts[$index];
        }
        return $host;
    }/*}}}*/
    private function getResponse($retry)
    {/*{{{*/
        $html = curl_exec($this->_ch);
        while(empty($html) && $retry > 0)
        {
            sleep($this->sleepTime);
            $html = curl_exec($this->_ch);
            $retry--;
        }
        return $html;
    }/*}}}*/
    private function setPostData($postData)
    {/*{{{*/
        curl_setopt($this->_ch, CURLOPT_POST, true);
        curl_setopt($this->_ch, CURLOPT_POSTFIELDS, $postData);
    }/*}}}*/
    private function clearPostStatus()
    {/*{{{*/
        curl_setopt($this->_ch, CURLOPT_HTTPGET, true);
    }/*}}}*/

    public function __destruct()
    {/*{{{*/
        curl_close($this->_ch);
        $this->_ch  = null;
        self::$_ins = null;
    }/*}}}*/
}/*}}}*/
