<?php error_reporting (E_ALL);

require_once( 'PayflowReports.php' );


/**
 * Each Search report needs to extend PayflowSearchQuery with runSearchRequest() and parseSearchResponse()
 */
class PayflowSearchQuery extends PayflowReportQuery {

	public function runSearch() {
		$xml = $this->runSearchRequest();
		$request = $this->createRequest( $xml );
	
		$rc = $this->sendRequest( $request );
		$xml_response = new SimpleXMLElement( $rc );
		$responseCode = $xml_response->baseResponse->responseCode;


		switch ( $responseCode ) {
			case 100:
				$this->reportId = $xml_response->runSearchResponse->reportId;
				$result = $this->parseSearchResponse( $xml_response );
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
			default:
				print_r($xml_response);
				die( "Blowing up as we got response code $responseCode\n" );
		}   

		return $result;
	}
}
	
/**
 * Search for a given transaction
 *
 */

class TransactionIDSearch extends PayflowSearchQuery {
	public $reportName = 'TransactionIDSearch';

	public function __construct( $id ) {
		$this->trxn_id = $id;
		$this->loadAuthData();
	}

	public function runSearchRequest() {
		$options = array( 
				'transaction_id' => $this->trxn_id,
				'timezone' => 'GMT',
			 );
		
		$xml  = '<runSearchRequest>';
		$xml .= $this->xmlTag( 'searchName', 'TransactionIDSearch' );
		$xml .= $this->addParams( $options );
		$xml .= '</runSearchRequest>';

		return $xml;
	}

	public function parseSearchResponse ( $xml_response ) {
		$currency = CurrencyConversion::init();
		$country_list = CountryCodeConversion::init();

		$response = $this->getMetadataRequest();
			print_r($response);
		
		$xml_response = new SimpleXMLElement( $response );
		
		if ( $this->readMetaDataRequest( $xml_response ) ) {
			$response = $this->getDataRequest( $this->reportId, 1 );
		
			try {
				$xml_response = new SimpleXMLElement( $response );
			} catch ( Exception $e ) {
				echo "Bad record from report: $this->reportId\n";
			}

			print_r($xml_response);
			foreach( $xml_response->getDataResponse->reportDataRow as $row ) {
				$first_name = $row->columnData[14]->data;
				$last_name  = $row->columnData[15]->data;
				//$street     = $row->columnData[16]->data;
				$street     = $row->columnData[24]->data;
				//$postal     = $row->columnData[19]->data;
				$postal     = $row->columnData[27]->data;
	
				$city = '';
				$state = '';
				$country = '';

				if ( $row->columnData[20]->data == '' ) {
					$rc = GoogleMapsHttp::lookupAddress( $street, $postal );
				
					$xml_response = new SimpleXMLElement( $rc );
					$xml_response->registerXPathNamespace( 'kml','http://earth.google.com/kml/2.0' );
					if ( $xml_response->Response->Status->code == 200 && ! $xml_response->xpath( '//kml:Placemark[@id="p2"]' ) ) {
						$city = ( isset( $xml_response->Response->Placemark->AddressDetails->Country->AdministrativeArea->SubAdministrativeArea ) ) ? $xml_response->Response->Placemark->AddressDetails->Country->AdministrativeArea->SubAdministrativeArea->Locality->LocalityName : $xml_response->Response->Placemark->AddressDetails->Country->AdministrativeArea->Locality->LocalityName;
						$state = ( isset ($xml_response->Response->Placemark->AddressDetails->Country->AdministrativeArea->AdministrativeAreaName) ) ? $xml_response->Response->Placemark->AddressDetails->Country->AdministrativeArea->AdministrativeAreaName : '';
						$country = $country_list{ (string ) $xml_response->Response->Placemark->AddressDetails->Country->CountryNameCode};
					}
				} else {
					$city       = $row->columnData[17]->data;
					$state      = $row->columnData[18]->data;
					$country    = $row->columnData[20]->data;
				}
					
				$sup_add    = '';
				$contrib_type = "Cash";
				$total      = number_format( $row->columnData[11]->data / 100, 2, '.', '' );
				$recieve_date = date( 'm/d/Y' , strtotime( $row->columnData[3]->data ) );
				//$recieve_date = date( 'F d, Y' , strtotime( $row->columnData[3]->data ) );
				$payment_instr = "Gateway";
				$contrib_src = trim( $row->columnData[40]->data );
				$converted_total = number_format( $total * $currency[$contrib_src], 2, '.', '' );
				$orig_string = $row->columnData[40]->data . " " . $total;
				$input_currency = 'USD';
				$tender_type = $row->columnData[6]->data;

				$payment = '';
				if ( $tender_type == 'PayPal' ) {
					$payment = 'PAYPAL';
				} else if ( $tender_type == 'American Express' || 'Visa' || 'MasterCard' || 'Discover' ) {
					$payment = 'PAYFLOWPRO';
				}
				$trxn_id	= $row->columnData[0]->data;
				$trxn_id_string = $payment . ' ' . $trxn_id;
				$fund        = 'unrestricted';
				$campaign    = 'communitygift';
				$appeal      = 'spontaneousdonation';
				$letter_code = 'General';
				$thank_you_s = 'Development_Stage';
				$email = $row->columnData[21]->data;
				$line = $trxn_id . "," . $first_name . "," . $last_name . "," . '"' . $street . '"' . "," . 
					$sup_add . "," .
					$city . "," . $state . "," . $postal . "," . $country . "," . 
					$contrib_type . "," . $converted_total . "," . '"' . $recieve_date . '"' . "," . 
					$payment_instr . "," . $input_currency . "," . $trxn_id_string . "," .
					$fund . "," . $campaign . "," . $appeal . "," . $letter_code . "," .
					$thank_you_s . "," . '"' . $orig_string . '"' . "," . $email;;
				print $line . "\n";
			}
		}
		
	}

	public function saveReportInfo() {
	}
}

class FraudTransactionSearch extends PayflowSearchQuery {
	private $reportName = 'FraudTransactionSearch';

	public function __construct( $id ) {
		$this->trxn_id = $id;
		$this->loadAuthData();
	}

	public function runSearchRequest() {
		$options = array( 
				'fraud_transaction_id' => $this->trxn_id,
				'transaction_type' => 'Sale'
		);
		$xml  = '<runSearchRequest>';
		$xml .= $this->xmlTag( 'searchName', 'FraudTransactionSearch' );
		$xml .= $this->addParams( $options );
		$xml .= '</runSearchRequest>';

		return $xml;
	}

	public function parseSearchResponse ( $xml_response ) {
		$responseCode = $xml_response->baseResponse->responseCode; // if xml fails its statusCode
		
		switch ( $responseCode ) {
			case 100:
				$this->reportId = $xml_response->runSearchResponse->reportId;
				$status = $xml_response->runReportResponse->statusCode;
				break;
			case 129: 
				die( "Error 129: " . $xml_response->baseResponse->responseMsg . "\n" );
			default:
				print_r($xml_response);
				die( "Blowing up as we got response code $responseCode\n" );
		}

		$response = $this->getMetadataRequest( $this->reportId );

		$xml_response = new SimpleXMLElement( $response );

		if ( $this->readMetaDataRequest( $xml_response ) ) {
			$response = $this->getDataRequest( $this->reportId, 1 ); // Searches never have more then 1 page

			try {
				$xml_response = new SimpleXMLElement( $response );
			} catch ( Exception $e ) {
				echo "Bad record from report: $id\n";
			}
			foreach( $xml_response->getDataResponse->reportDataRow as $row ) {
				print_r($row);
			}
		}
	}

}

class PayPalTransactionIdSearch extends PayflowSearchQuery {
	public $reportName = 'PayPalTransactionIDSearch';

	public function __construct( $id ) {
		$this->paypal_trxn_id = $id;
		$this->loadAuthData();
	}

	public function runSearchRequest() {
		$options = array(
				'paypal_transaction_id' => $this->paypal_trxn_id,
				'timezone' => 'GMT',
		);

		$xml  = '<runSearchRequest>';
		$xml .= $this->xmlTag( 'searchName', 'PayPalTransactionIDSearch' );
		$xml .= $this->addParams( $options );
		$xml .= '</runSearchRequest>';

		return $xml;

	}

	public function parseSearchResponse ( $xml_response ) {
		$responseCode = $xml_response->baseResponse->responseCode; // if xml fails its statusCode
		
		switch ( $responseCode ) {
			case 100:
				$id = $xml_response->runSearchResponse->reportId;
				$status = $xml_response->runReportResponse->statusCode;
				break;
			case 129: 
				die( "Error 129: " . $xml_response->baseResponse->responseMsg . "\n" );
			default:
				print_r($xml_response);
				die( "Blowing up as we got response code $responseCode\n" );
		}

		$response = $this->getMetadataRequest( $id );

		$xml_response = new SimpleXMLElement( $response );

		if ( $this->readMetaDataRequest( $xml_response ) ) {
			$response = $this->getDataRequest( $this->reportId, 1 ); // Searches never have more then 1 page

			try {
				$xml_response = new SimpleXMLElement( $response );
			} catch ( Exception $e ) {
				echo "Bad record from report: $id\n";
			}
			foreach( $xml_response->getDataResponse->reportDataRow as $row ) {
				$payflow_trxn_id = $row->columnData[1]->data;
			}

			return $payflow_trxn_id;
		}
	}

}

class GoogleMapsHttp{
	public static function lookupAddress( $address, $zipcode ) {
		$url = 'http://maps.google.com/maps/geo';
		$key = 'ABQIAAAAzpt2uoSGGHkdr648tJfysxRvHbMBYluNysiq1oSLrBxKfS12gxTg_YENNv47Q-iGikajxVrnciD0iw';
		$sensor = 'false';
		$oe = 'utf8';
		$output = 'xml';

		$ch = curl_init();
		
		$full_address = "$address, $zipcode";

		$lookupUrl = $url . '?' . 'output=' . $output . '&' . 'oe=' . $oe . 
			   '&' . 'sensor=' . $sensor . '&' . 'key=' . $key . '&' .
			   'q=' . urlencode( $full_address );  

		curl_setopt( $ch, CURLOPT_URL, $lookupUrl );	
		curl_setopt( $ch, CURLOPT_RETURNTRANSFER, TRUE );

		$response = curl_exec( $ch );

		curl_close( $ch );
	
		return $response;
	}
}

class CurrencyConversion{
	public static function init() {
		$url = 'http://www.ecb.int/stats/eurofxref/eurofxref-daily.xml';
		$ilsUrl = 'http://www.bankisrael.gov.il/currency.xml';

		$xml_response = new SimpleXMLElement( $url, NULL, TRUE );
	
		foreach( $xml_response->Cube->Cube->children() as $unit ) {
			if ( $unit['currency'] == 'USD' ) {
				$usd_base = $unit['rate'];
			}
		}

		$rates{'EUR'} = (string) $usd_base;

		foreach( $xml_response->Cube->Cube->children() as $unit ) {
			$currency = (string) $unit['currency'];
			$rates{$currency} = $usd_base / (string) $unit['rate'];
		}

		$rates{'USD'} = 1;

		$xml_response = new SimpleXMLElement( $ilsUrl, NULL, TRUE );

		foreach( $xml_response->CURRENCY->children() as $unit ) {
			if ( $unit['NAME'] == 'Dollar' ) {
				$rates{'ILS'} = 1 / $unit['RATE'];
				break;
			}
		} 
		return $rates;
	}
} 

class CountryCodeConversion {
	public static function init() {

		$file = 'iso3166.txt';
		
		$fp = fopen( $file, 'r' );

		while ( ( $data = fgetcsv( $fp, 1000, ",") ) !== FALSE ) {
			$countries{$data[1]} = $data[0];
		}
		
		return $countries;
	}
}
?>
