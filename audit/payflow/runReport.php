<?php

require_once( 'PayflowReports.php' );
require_once( 'PayflowReportTypes.php' );
require_once( 'PayflowSearch.php' );

//$options = getopt("s:e:");

//$begin = date ( 'Y-m-d', strtotime( $options['s']) );
//$end = date ( 'Y-m-d', strtotime( $options['e']) );

// $report = new DailyActivityReport( array( $options['s'] ) );
//$report = new CustomReport( array ( $begin . ' 00:00:00', $end . ' 23:59:59') );

$report2 = new TransactionIDSearch ( 'EUYP6E528DE6' );
$report2 = $report2->runSearch();


?>
