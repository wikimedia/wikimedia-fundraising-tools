<?php

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

$start = time();

$settings = parse_ini_file('run_settings.ini', TRUE);

$refund = array(
	'METHOD' => 'RefundTransaction',
	'VERSION' => '95.0',
	'TRANSACTIONID' => '', //txn_id
	'REFUNDTYPE' => 'Full',
);

foreach ( $settings['account'] as $key => $val ){
	$refund[$key] = $val;
}

if ( !file_exists(__DIR__ . '/' . $settings['whatever']['cert'] ) ){
	die("Oh noes: Cert file does not exist.");
}

$db_conn = mysql_connect(
	$settings['db']['server'],
	$settings['db']['un'],
	$settings['db']['pw'] );
mysql_select_db( $settings['db']['db_name'], $db_conn );

$qry_refunded = 'refunded IS NULL';
if ($settings['whatever']['mode'] === 'repair'){
	$qry_refunded = 'refunded = 2';
}

$query = "SELECT * FROM kp_q2c_log WHERE $qry_refunded ORDER BY timestamp DESC LIMIT " . $settings['whatever']['limit'];
$result = mysql_query( $query );
while ( $row = mysql_fetch_assoc( $result )){
	echo print_r($row, true);
	$refund['TRANSACTIONID'] = $row['gateway_txn_id'];
	if ($settings['whatever']['mode'] === 'repair'){
		$json = $row['data'];
		$data = json_decode($json, true);
		$refund['TRANSACTIONID'] = $data['gateway_txn_id'];
	}
	
	$cres = curl_download( $settings['url']['prod'], $settings['whatever']['cert'], $refund  );
	
	//yoinked from DonationInterface
	$responseArray = array();
	$result_arr = explode( "&", $cres );
	foreach ( $result_arr as $result_pair ) {
		list( $key, $value ) = preg_split( "/=/", $result_pair, 2 );
		$responseArray[ $key ] = urldecode($value);
	}

	
	echo "Results from cURL: " . print_r( $responseArray, true ) . "\n";
	
	$refunded = false;
	if ( $responseArray['ACK'] === 'Success' ){
		$refunded = true;
	} elseif( $responseArray['L_LONGMESSAGE0'] === 'This transaction has already been fully refunded' ) {
		$refunded = true;
	}
	
	if ( $refunded ){
		echo 'Transaction ' . $refund['TRANSACTIONID'] . " successfully refunded!\n";
		$query = "UPDATE kp_q2c_log SET refunded=1 WHERE gateway_txn_id = '" . $refund['TRANSACTIONID'] . "'";
		if ($settings['whatever']['mode'] === 'repair'){
			$query = "UPDATE kp_q2c_log SET gateway_txn_id = '" . $refund['TRANSACTIONID'] . "', refunded=3 WHERE cid = '" . $row['cid'] . "'";
		}
		echo "$query\n";
		$ures = mysql_query( $query );
		echo "$ures\n";
	} elseif( $responseArray['L_LONGMESSAGE0'] === 'The transaction id is not valid' ) {
		echo 'Transaction ' . $refund['TRANSACTIONID'] . " NOT refunded! Dirty data! Dirty dirty dirty!\n";
		$query = "UPDATE kp_q2c_log SET refunded=2 WHERE gateway_txn_id = '" . $refund['TRANSACTIONID'] . "'";
		echo "$query\n";
		$ures = mysql_query( $query );
		echo "$ures\n";
	}
	
}


$end = time();
$duration = $end - $start;
$average = ceil( $duration / $settings['whatever']['limit'] );

die("Processed " . $settings['whatever']['limit'] . " records in $duration seconds.\nThat's about " . $average . " seconds per record.\n hrrgrgrgrk.\n");


/**
 * Connect to a URL, send optional post variables, return data
 *
 * Yoinked most recently from the paypal IPN listener in svn. 
 * @param $url String of the URL to connect to
 * @param $vars Array of POST variables
 * @return String containing the output returned from Server
 */
function curl_download( $url, $cert, $vars = NULL ) {
	$ch = curl_init();
	curl_setopt($ch, CURLOPT_URL, $url);
	curl_setopt($ch, CURLOPT_HEADER, 0); 
	curl_setopt($ch, CURLOPT_FOLLOWLOCATION, 0);
	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 1);
	curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 1);
	curl_setopt($ch, CURLOPT_CAINFO, '/etc/ssl/certs/ca-certificates.crt' );
	curl_setopt($ch, CURLOPT_SSLCERT, $cert );
	//curl_setopt($ch, CURLOPT_CAPATH, __DIR__ );

	if ($vars !== NULL) {
		$post_string = '';
		foreach( $vars as $field => $value ){
			if( function_exists( 'get_magic_quotes_gpc' ) == true && get_magic_quotes_gpc() == 1){
				$value = urlencode( stripslashes( $value ) );
			}else{
				$value = urlencode( $value );
			}
			$post_string .= $field . '=' . $value . '&';
		}
		$post_string .= "cmd=_notify-validate";

		curl_setopt( $ch, CURLOPT_POST, 1 );
		curl_setopt( $ch, CURLOPT_POSTFIELDS, $post_string );
	}

	$i = 0;

	while (++$i <= 3){
		$data = curl_exec($ch);
		$header = curl_getinfo($ch);

		if ( $header['http_code'] != 200 && $header['http_code'] != 403 ){
			//paypal blow'd up.
			sleep( 1 );
		}

		if (!$data) {
			$data = curl_error($ch);
			echo "Curl error: " . $data . "\n";
		} else {
			break;
		}

	}
	curl_close($ch);
	return $data;
}