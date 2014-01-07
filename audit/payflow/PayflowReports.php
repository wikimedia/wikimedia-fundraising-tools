<?php error_reporting (E_ALL);

/*
 * Base class for a returned Payflow Report. This class is used by all others to access Payflow report data.
 *
 */

class PayflowReport {
	public $reportName;
	public $results = array();
	protected $columnFilters = array();
	protected $reportResponse;
	
	public function __construct( $report ) {
		$this->reportName = $report;
	}

	/** 
	 * Add the fully parsed and preapred report response
	 * @param ReportResponse $reportResponse
	 */
	public function addReportResponse( ReportResponse $reportResponse ) {
		$this->reportResponse = $reportResponse;
	}

	public function getReportResponse() {
		return $this->reportResponse;
	}
	
	/**
	 * Specify the columns filters you wish to apply when viewing the report 
	 * @param array $filters
	 * @TODO perhaps do some validity checking of requested filters
	 */
	public function setColumnFilters( array $filters ) {
		$this->columnFilters = $filters;
	}
	
	/**
	 * Fetch an iterable object of column data
	 * 
	 * Sets any defined column filters on objects returned.
	 * 
	 * @return object
	 */
	public function getColumns() {
		if ( !$this->checkReportResponse() ) {
			throw new Exception( 'ReportResponse does not exist.' );
		}
		
		if ( count( $this->columnFilters )) {
			$this->reportResponse->columns->setFilterKeys( $this->columnFilters );
		}
		
		return $this->reportResponse->columns;
	}
	
	/**
	 * Fetch an array of iterable data row objects
	 * 
	 * Sets any defined column filters on objects returned.
	 * 
	 * @return array
	 */
	public function getData() {
		if ( !$this->checkReportResponse() ) {
			throw new Exception( 'ReportResponse does not exist.' );
		}
		
		if ( count( $this->columnFilters )) {
			foreach( $this->reportResponse->rows as $row ) {
				$row->setFilterKeys( $this->columnFilters );
			}
		}
		
		return $this->reportResponse->rows;
	}
	
	/**
	 * Check existence of report response
	 * @TODO add additional checks for things like data and columns
	 * @return bool
	 */
	public function checkReportResponse() {
		if ( !$this->reportResponse ) {
			return false;
		}
		
		return true;
	}
}

/*
 * Subclass for sending a report specific query to PayFlow
 *
 */

class PayflowReportQuery extends PayflowQuery {

	protected $reportResponse;
	public $pageSize = 50;
	
	public function __construct( $date = null ) {
		require_once( dirname(__FILE__) . "/PayflowReportResponse.php" );
		$this->date['start'] = isset( $date[0] ) ? $date[0] : date( 'Y-m-d' ); 
		$this->date['end'] = isset( $date[1] ) ? $date[1] : date( 'Y-m-d' );
	}

	// Get the results of a previous run report from payflow
	public function getReportData( $id = null ) {
		$id = ( is_null( $id ) ) ? $this->reportId : $id;
		$xml  = '<getResultsRequest>';
		$xml .= $this->xmlTag( 'reportId', $id );
		$xml .= '</getResultsRequest>';

		$request = $this->createRequest( $xml );

		$rc = $this->sendRequest( $request );

		return $rc;
	}

	// Retrieve all the metadata for a given report
	public function getMetadataRequest() {
		$xml  = '<getMetaDataRequest>';
		$xml .= self::xmlTag( 'reportId', $this->reportId );
		$xml .= '</getMetaDataRequest>';

		$request = $this->createRequest( $xml );

		$rc = $this->sendRequest( $request );

		return $rc;
	}

	// Retrieves a specfic page from a given report
	public function getDataRequest( $id, $page ) {
		$xml  = '<getDataRequest>';
		$xml .= $this->xmlTag( 'reportId', $id );
		$xml .= $this->xmlTag( 'pageNum' , $page ); // Will this ever not be 1
		$xml .= '</getDataRequest>';

		$request = $this->createRequest( $xml );
		$response = $this->sendRequest( $request );

		return $response;
	}

	// Actually run the report
	public function runReport() {
		$xml = $this->runReportRequest();
		$request = $this->createRequest( $xml );

		$rc = $this->sendRequest( $request );
		$xml_response = new SimpleXMLElement( $rc );

		$responseCode = $xml_response->baseResponse->responseCode;

		switch ( $responseCode ) {
			case 100:
				$this->reportId = $xml_response->runReportResponse->reportId;
				$response = $this->parseReportResponse( $xml_response );
				// $response->printEntries();
				break;
			case 101:
				die( "Request has failed\n" );
			case 102:
				die( "An internal scheduler error has occurred\n" );
			case 103:
				die( "Unknown report requested\n" );
			case 104:
				die( "Invalid Report ID\n" );
			case 105: 
				die( "A system error has occurred\n" );
			case 106:
				die( "A database error has occurred\n" );
			case 107:
				die( "Invalid XML request\n" );
			case 108:
				die( "User authentication failed\n" );
			case 109:
				die( "Invalid report parameters provided\n" );
			case 110:
				die( "Invalid merchant account\n" );
			case 111:
				die( "Invalid page number\n" );
			case 112:
				die( "Template already exists\n" );
			case 113:
				die( "Unknown template requested\n" );
			case 129: 
				$this->splitReport();
				break;
			case 131:
				print "Warning: Waiting as Report is still being created\n";
				sleep(10);
				$this->runReport();
				break;
			default:
				print_r($xml_response);
				die( "Blowing up as we got response code $responseCode\n" );
		}
		return $response;
	}

	// Parse the results from a payflow report
	public function parseReportResponse ( $xml_response ) {
		$statusCode = $xml_response->runReportResponse->statusCode;

		switch ( $statusCode ) {
			case 1: // Created
				return;
			case 2: // Still executing
				while(1) {
					print( "Waiting as report is still running $this->reportId \n" );
					sleep(60);
					if ( $this->isReportReady() ) {
						break;
					}
				}
				break;
			case 3: // Done
				break;	
			case 4: // Failed
				die( "Report Creation Failed\n" );
			case 5: // Expired
				die( "Expired" );
			case 6: // Expired?
				die( "Expired" );
			default:
		}
	
		$this->saveReportInfo();
		
		// return the metadata as a SimpleXMLEelement object
		$simple_xml_metadata = new SimpleXMLElement( $this->getMetadataRequest() );
		
		/**
		 * begin building a report response object
		 * this will contain parsed report data for us to use later.
		 */ 
		if ( !$this->reportResponse ) {
			$this->reportResponse = new ReportResponse();
		}

		// parse the metadata
		$this->reportResponse->parseMetaData( $simple_xml_metadata );
		
		for( $i = 1; $i <= $this->reportResponse->getNumberOfPages(); $i++ ) {
			// loop through pages and fetch their XML
			$response = $this->getDataRequest( $this->reportId, $i );

			try {
				$xml_response = new SimpleXMLElement( $response );
				$this->reportResponse->addData( $xml_response );
			} catch ( Exception $e ) {
				echo "Bad XML. Skipping page $i from report: $this->reportId\n";
			}
		}
		
		return true;
	}

	// Query Payflow to find out if our report has finished running. Some reports take a while to run.
	public function isReportReady() {
		$rc = $this->getReportData();	

		$xml = new SimpleXMLElement( $rc );
		
		if ( $xml->runReportResponse->statusCode == 3 ) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Save what we've been doing to a log
	 * 
	 * @TODO make this better, perhaps with syslog support
	 */
	public function saveReportInfo() {
		$string = $this->reportName . ',' . $this->reportId . ',' . $this->date['start'] . ',' . 
			  $this->date['end'] . "\n";

		if ( $fp = @fopen( '/var/log/fundraising/contrib_audit/log', 'a' )) {
			fwrite( $fp , $string );
			fclose( $fp );
		}
	}

	// If paypal reports that our query is too big we have to split it up. Divide the time by 2 and try again
	// till we get it right
	public function splitReport() {
		$start_end = strtotime( $this->date['start'] );
		$split_end = strtotime( $this->date['end'] );

		$diff = $split_end - $start_end;
		$middle = date( 'Y-m-d H:i:s', ( $start_end + ( $diff / 2 ) ) );

		print "Warning: Halving report length from: " . date( 'Y-m-d H:i:s', $start_end ) . " to: $middle\n";

		$report = new CustomReport( array( $this->date['start'], $middle ) );
		$report->runReport();
		$report2 = new CustomReport( array( $middle, $this->date['end'] ) );
		$report2->runReport();
	}

	/**
	 * Fetch the results as a monolitich report object.
	 */
	public function getResults() {
		// instantiate our report object
		$results = new PayflowReport( $this->reportName );
		
		// feed it the reportResponse object, 
		//which contains all of the data from which to build the report
		$results->addReportResponse( $this->reportResponse );
		return $results;
	}
}

/*
 * Top level Class to Query Payflow
 *
 */

class PayflowQuery {

	private $payflowTest = 'https://payments-reports.paypal.com/test-reportingengine';
	private $payflowLive = 'https://payments-reports.paypal.com/reportingengine';

	private $header = '<?xml version="1.0" encoding="UTF-8"?>';

	/**
	 * An array containing the input params for a particular report
	 * 	$options[<param name>] = <param value>
	 * @var array
	 */
	protected $options = array();
	
	/**
	 * An array containing required parameter names for a particular report
	 * 	$reqd_options = array( 'start_date', 'end_date' );
	 * @var array
	 */
	protected $reqd_options = array();
	
	public function __construct() {
		$this->loadAuthData();
	}

	public function loadAuthData() {
		$config = 'auth.cfg';
		$auth = parse_ini_file( $config );

		$this->vendor = $auth['vendor'];
		$this->partner = $auth['partner'];
		$this->user = $auth['user'];
		$this->password = $auth['password'];
	}

	// Escape and all approprite tags
	protected function xmlTag( $tag, $value ) {
		$xml  = "<$tag>" . htmlspecialchars( $value ) . "</$tag>";

		return $xml;
	}

	// Create new reporting engine request
	public function createRequest( $data ) {
		$xml  = $this->header;
		$xml .= '<reportingEngineRequest>';
		$xml .= $this->authRequest();
		$xml .= $data;
		$xml .= '</reportingEngineRequest>';
		
		return $xml;
	}

	// Create a new auth request
	public function authRequest() {
		$xml  = '<authRequest>';
		$xml .= '<user>' . $this->user . '</user>';
		$xml .= '<vendor>' . $this->vendor . '</vendor>';
		$xml .= '<partner>' . $this->partner . '</partner>';
		$xml .= '<password>' . $this->password . '</password>';
		$xml .= '</authRequest>';
		
		return $xml;
	}

	// Build the report request
	public function runReportRequest() {
		$xml  = '<runReportRequest>';
		$xml .= self::xmlTag( 'reportName', $this->reportName );
		$xml .= $this->addParams( $this->getOptions() );
		$xml .= '</runReportRequest>';

		return $xml;
	}
	
	// Send a request to paypal
	public function sendRequest( $xml ) {
		//print "Sending: $xml\n";

		$ch = curl_init();
		curl_setopt( $ch, CURLOPT_URL, $this->payflowLive );
		curl_setopt( $ch, CURLOPT_HEADER, 0 );
		curl_setopt( $ch, CURLOPT_POSTFIELDS, $xml );
		curl_setopt( $ch, CURLOPT_POST, 1 );
		curl_setopt( $ch, CURLOPT_SSL_VERIFYPEER, FALSE );
		curl_setopt( $ch, CURLOPT_RETURNTRANSFER, TRUE );
		
		$response = curl_exec( $ch );
		$headers = curl_getinfo( $ch );
		
		//print "Got $response\n";

		curl_close( $ch );

		return $response; 
	}

	// Given an options array create the paramName and paramValue parts of a report
	public function addParams( $options = array() ) {
		$xml = '';

		foreach( $options as $name => $value ) {
			$xml .= '<reportParam>';
			$xml .= self::xmlTag( 'paramName', $name );
			$xml .= self::xmlTag( 'paramValue', $value );
			$xml .= '</reportParam>'; 
		}
		return $xml;
	}
	
	public function setOptions( array $options ) {
		$this->options = $options;
	}
	
	/**
	 * Fetch the parameters and options for a query
	 * @param bool $ignore_reqd Wheh set to true, sanity check will not be performed
	 * @throws Exception
	 * @return array
	 */
	public function getOptions( $ignore_reqd=false ) {
		if ( !$ignore_reqd && !$this->checkSanityOptions()) {
			throw new Exception( 'Missing required parameter in report: ' . get_class( $this ));
		}
		
		return $this->options;
	}
	
	/**
	 * Perform sanity check on options array
	 * 
	 * Compares keys in options array against list of required params
	 * @return bool
	 */
	public function checkSanityOptions() {
		foreach ( $this->reqd_options as $reqd_option ) {
			if ( !in_array( $reqd_option, array_keys( $this->options ))) {
				return false;				
			}
		}
		
		return true;
	}
	
	public function setReqdOptions( array $reqd_options ) {
		$this->reqd_options = $reqd_options;
	}
	
	public function getReqdOptions() {
		return $this->reqd_options;
	}
}
?>
