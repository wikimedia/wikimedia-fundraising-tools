<?php 
require_once( 'Wr1DataRecord_CreditCard.php' );

/**
 * Parsing class for "XON" records, which are for approved (but not yet
 * settled) credit card transactions.
 */
class Wr1DataRecord_CreditCard_Processed extends Wr1DataRecord_CreditCard {
	
	/**
	 * @see Wr1DataRecord::fieldPositionMap
	 */
	private $processedCreditCardFieldPositionMap = array(
		'Record-category' => array( 0, 1 ),
		'Record-type' => array( 1, 3 ),
		'Reserved_0' => array( 3, 15 ),
		'Order-number' => array( 15, 45 ),
		'Customer-ID' => array( 45, 60 ),
		'Reserved_1' => array( 60, 91 ),
		'Transaction-currency' => array( 91, 95 ),
		'Reserved_2' => array( 95, 101 ),
		'Transaction-amount' => array( 101, 113 ),
		'Amount-sign_transaction' => array( 113, 114 ),
		'Reserved_3' => array( 114, 131 ),
		'Date-authorised' => array( 131, 139 ),
		'Reserved_4' => array( 139, 167 ),
		'Card-number' => array( 167, 186 ),
		'Reserved_5' => array( 186, 190 ),
		'Expiry-date' => array( 190, 194 ),
		'Issue-number' => array( 194, 196 ),
		'Source-ID' => array( 196, 200 ),
		'Authorisation-code' => array( 200, 208 ),
		'WbC Payment method ID' => array( 208, 210 ),
		'WbC Paymentproduct ID' => array( 210, 214 ),
		'Reserved_5' => array( 214, 220 ),
		'Payment-method' => array( 220, 222 ),
		'Creditcard-company' => array( 222, 226 ),
		'Unclean-indicator' => array( 226, 227 ),
		'Payment-currency' => array( 227, 231 ),
		'Payment-amount' => array( 231, 243 ),
		'Amount-sign_payment' => array( 243, 244 ),
		'Currency-due' => array( 244, 248 ),
		'Amount-due' => array( 248, 260 ),
		'Amount-sign_amount' => array( 260, 261 ),
		'Date-due' => array( 261, 269 ),
		'Authorisation-indicator' => array( 269, 270 ),
		'Filler_0' => array( 270, 320 ),
	);
	
	/**
	 * Get local field position map
	 */
	public function getLocalFieldPositionMap() {
		return $this->getProcessedCreditCardFieldPositionMap();
	}
	
	public function getProcessedCreditCardFieldPositionMap() {
		return $this->processedCreditCardFieldPositionMap;
	}
}
