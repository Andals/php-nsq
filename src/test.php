<?php
/**
 * Created by IntelliJ IDEA.
 * User: ligang
 * Date: 2017/8/8
 * Time: 16:31
 */

$socket = stream_socket_client("tcp://127.0.0.1:4150");
stream_socket_sendto($socket, "  V2");
stream_socket_sendto($socket, "SUB app2_t2 c1\n");

$bsize = stream_socket_recvfrom($socket, 4);
$asize = unpack('N', $bsize);
$size = current($asize);

//$bdata = stream_socket_recvfrom($socket, $size);
//$frame = unpack('N', substr($bdata, 0, 4));
//
//$chars = unpack('c2', substr($bdata, 4));
//$response = '';
//foreach ($chars as $char) {
//    $response.=chr($char);
//}
//var_dump($response);

$address = 'tcp://127.0.0.1:4890';
$bdata = stream_socket_recvfrom($socket, $size, 0, $address);
var_dump($bdata);
