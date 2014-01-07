<?php 
require_once( 'Wr1DataRecord.php' );

/**
 * Parsing class for "+IP" records, which are for settled 'invoice payments'.
 * 
 * 'Invoice payments' comprise Global Collect payment methods other than credit
 * card and direct debit.
 */
class Wr1DataRecord_InvoicePayment_Settled extends Wr1DataRecord {
	
	/**
	 * @see Wr1DataRecord::fieldPositionMap
	 */
	private $processedInvoicePaymentFieldPositionMap = array(
		'Record-category' => array( 0, 1 ),
		'Record-type' => array( 1, 3 ),
		'Payment-reference' => array( 3, 15 ),
		'Invoice-number' => array( 15, 35 ),
		'Customer-ID' => array( 35, 50 ),
		'Additional-reference' => array( 50, 70 ),
		'Effort-number' => array( 70, 71 ),
		'Invoice-currency-deliv' => array( 71, 81 ),
		'Invoice-amount-deliv' => array( 81, 93 ),
		'Amount-sign_deliv' => array( 93, 94 ),
		'Invoice-currency-local' => array( 94, 98 ),
		'Invoice-amount-local' => array( 98, 110 ),
		'Amount-sign_local' => array( 110, 111 ),
		'Reserved_0' => array( 111, 188 ),
		'WbC Payment method ID' => array( 188, 190 ),
		'WbC Payment product ID' => array( 190, 194 ),
		'Reserved_1' => array( 194, 200 ),
		'Payment-method' => array( 200, 202 ),
		'Creditcard-company' => array( 202, 206 ),
		'Unclean-indicator' => array( 206, 207 ),
		'Payment-currency' => array( 207, 211 ),
		'Payment-amount' => array( 211, 223 ),
		'Amount-sign_payment' => array( 223, 224 ),
		'Currency-due' => array( 224, 228 ),
		'Amount-due' => array( 228, 240 ),
		'Amount-sign_due' => array( 240, 241 ),
		'Date-due' => array( 241, 249 ),
		'Over-under-currency-local' => array( 249, 253 ),
		'Over-under-amount-local' => array( 253, 265 ),
		'Amount-sign_over-under' => array( 265, 266 ),
		'Filler' => array( 266, 400 ),
	);
	
	/**
	 * Get local field position map
	 */
	public function getLocalFieldPositionMap() {
		return $this->getProcessedInvoicePaymentFieldPositionMap();
	}
	
	public function getProcessedInvoicePaymentFieldPositionMap() {
		return $this->processedInvoicePaymentFieldPositionMap;
	}
}