<?php 
require_once( 'Wr1DataRecord.php' );

/**
 * Parsing class for "+AP" records, which are for settled Direct Debit payments.
 * Obviously, the "A" stands for "Direct Debit".
 */
class Wr1DataRecord_DirectDebitPayment_Settled extends Wr1DataRecord {

	/**
	 * @see Wr1DataRecord::fieldPositionMap
	 */
	private $processedDirectDebitFieldPositionMap = array (
		'Record-category' => array( 0, 1 ),
		'Record-type' => array( 1, 3 ),
		'Payment-reference' => array( 3, 15 ),
		'Invoice-number' => array( 15, 35 ),
		'Customer-ID' => array( 35, 50 ),
		'Additional-reference' => array( 50, 70 ),
		'Effort-number' => array( 70, 71 ),
		'Order-currency-deliv' => array( 71, 81 ), // Invoice-currency-deliv
		'Order-amount-deliv' => array( 81, 93 ), // Invoice-amount-deliv
		'Amount-sign_deliv' => array( 93, 94 ),
		'Order-currency-local' => array( 94, 98 ), //Invoice-currency-local
		'Order-amount-local' => array( 98, 110 ), //Invoice-amount-local
		'Amount-sign_local' => array( 110, 111 ),
		'Date-order' => array( 111, 119 ), //new - where did date come from before?
		'Reserved_0' => array( 119, 148 ),
		'Account-debtor-number' => array( 148, 178 ), //new
		'Date-collect' => array( 178, 188 ), //new
		'WbC Payment method ID' => array( 188, 190 ),
		'WbC Payment product ID' => array( 190, 194 ),
		'Reserved_1' => array( 194, 200 ),
		'Payment-method' => array( 200, 202 ),
		'Reserved_2' => array( 202, 206 ),
		'Unclean-indicator' => array( 206, 207 ),
		'Payment-currency' => array( 207, 211 ),
		'Payment-amount' => array( 211, 223 ),
		'Amount-sign_payment' => array( 223, 224 ),
		'Currency-due' => array( 224, 228 ),
		'Amount-due' => array( 228, 240 ),
		'Amount-sign_due' => array( 240, 241 ),
		'Date-due' => array( 241, 249 ),
		'Reserved_3' => array( 249, 300 ),
		'Filler' => array( 300, 400 ),
	);
	
	/**
	 * Get local field position map
	 */
	public function getLocalFieldPositionMap() {
		return $this->getProcessedDirectDebitFieldPositionMap();
	}
	
	public function getProcessedDirectDebitFieldPositionMap() {
		return $this->processedDirectDebitFieldPositionMap;
	}
}