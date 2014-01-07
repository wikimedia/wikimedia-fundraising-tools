<?php 

require_once( 'PayflowReports.php' );
require_once( 'PayflowReportTypes.php' );
require_once( 'PayflowSearch.php' );


/*
 * Simple audit tool that runs a custom report and then first checks if a PayPal transaction id exists
 * If it doesn't find a PayPal id then it checks Payflow. If it doesn't find a Payflow id, then it complains.
 */

class PayFlowAudit {

	// Simple runner
	public function runAudit( $begin, $end ) {
		$report = new CustomReport( array ( $begin . ' 00:00:00', $end . ' 23:59:59') );
		if ( $report->runReport()) {
			$results = $report->getResults();           
		} else {
			die ( 'Something went horribly wrong.' );
		}

		$found = 0;
		$missing = 0;

		foreach( $results->results as $row ) {
			if ( ! $this->inCivi( $row[0] ) ) {
				//$report = new TransactionIDSearch( $row[0] );
				$report = new PayPalTransactionIDSearch( $row[0] );
				$secondary_id = $report->runSearch();
				
				// We do a secondary lookup if we first don't find the PayFlow trxn id
				if ( $secondary_id ) {
					if ( $this->inCivi( $secondary_id ) ) {
						print ("FoundS $row[0]\n");
						$found++;
					} else {
						print "MissingS $row[0]\n";
						$missing++;
					}
				} else {
					print "Mising id for $row[0]\n";
				}
			} else {
				print ("Found $row[0]\n");
				$found++;
			}
		}

		print "Found $found, Missing $missing\n";

	}

	// Civi lookup function
	public function inCivi( $id ) {
		$conf = parse_ini_file( 'auth.cfg' );

		$db = mysql_connect( $conf['db_server'], $conf['db_user'], $conf['db_pass']);
		if ( ! $db ) {
			die( "Couldn't connect: " . mysql_error() );
		};
		mysql_select_db("civicrm", $db);

		$result = mysql_query("SELECT id from trxn_id WHERE trxn_id = '$id'", $db);
		if ( ! $result ) {
			die ( "BOOM!" . mysql_error() . "\n" );
		}
		if ( mysql_num_rows($result) < 1 ) {
			return false;
		} else { 
			return true;
		}
	}

}

$options = getopt("s:e:");

$begin = date ( 'Y-m-d', strtotime( $options['s']) );
$end = date ( 'Y-m-d', strtotime( $options['e']) );

$runner = new PayFlowAudit();
$runner->runAudit( $begin, $end );

?>
