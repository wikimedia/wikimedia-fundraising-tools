<?php 
require_once( 'Wr1DataRecord.php' );

/**
 * Parent clasee for credit-card related transactions.
 * 
 * This almost certainly will need to be extended by subclasses for specific
 * card card transaction-type records from the wr1 file.
 */
class Wr1DataRecord_CreditCard extends Wr1DataRecord {

	/**
	 * @see Wr1DataRecord::fieldPositionMap
	 */
	private $creditCardFieldPositionMap = array(
		'Record-category' => array( 0, 1 ),
		'Record-type' => array( 1, 3 ),
		'Reserved_0' => array( 3, 15 ),
		'Order-number' => array( 15, 45 ),
		'Customer-ID' => array( 45, 60 ),
		'Reserved_1' => array( 60, 61 ),
		'Reference-original-payment' => array( 61, 91 ),
		'Transaction-currency' => array( 91, 95 ),
		'Reserved_2' => array( 95, 101 ),
		'Transaction-amount' => array( 101, 113 ),
		'Amount-sign_transaction' => array( 113, 114 ),
		'Reserved_3' => array( 114, 131 ),
		'Date-authorised' => array( 131, 139 ),
		'Declined-reason-code' => array( 139, 142 ),
		'Declined-reason-desc' => array( 142, 167 ),
		'Card-number' => array( 167, 186 ),
		'Reserved_4' => array( 186, 190 ),
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
		'Authorisation-code' => array( 270, 278 ),
		'Filler_0' => array( 278, 320 ),
	);
	
	public function getLocalFieldPositionMap() {
		return $this->creditCardFieldPositionMap;
	}
	
}
