<?php 
require_once( 'Wr1DataRecord_CreditCard.php' );

/**
 * Parsing class for "-CR" records, which are refunds on previous 
 * +ON records
 */
class Wr1DataRecord_CreditCard_Refund extends Wr1DataRecord_CreditCard {
	
	/**
	 * @see Wr1DataRecord::fieldPositionMap
	 */
	private $refundCreditCardFieldPositionMap = array(
		'Record-category' => array( 0, 1 ),
		'Record-type' => array( 1, 3 ),
		'Reserved_0' => array( 3, 15 ),
		'Order-number' => array( 15, 45 ),
		'Reserved_1' => array( 45, 61 ),
		'Reference-original-payment' => array( 61, 91 ),
		'Transaction-currency' => array( 91, 95 ),
		'Reserved_2' => array( 95, 101 ),
		'Transaction-amount' => array( 101, 113 ),
		'Amount-sign' => array( 113, 114 ),
		'Reserved_3' => array( 114, 139 ),
		'Declined-reason-code' => array( 139, 142 ),
		'Declined-reason-desc' => array( 142, 167 ),
		'Card-number' => array( 167, 186 ),
		'Reserved_4' => array( 186, 187 ),
		'Payment-processor-id' => array( 187, 190 ),
		'Expiry-date' => array( 190, 194 ),
		'Issue-number' => array( 194, 196 ),
		'Reserved_5' => array( 196, 208 ), //their documentation sucks
		'WbC Payment method ID' => array( 208, 210 ),
		'WbC Paymentproduct ID' => array( 210, 214 ),
		'Reserved_6' => array( 214, 220 ),
		'Payment-method' => array( 220, 222 ),
		'Creditcard-company' => array( 222, 226 ),
		'Unclean-indicator' => array( 226, 227 ),
		'Refund-currency' => array( 227, 231 ),
		'Refund-amount' => array( 231, 243 ),
		'Amount-sign_payment' => array( 243, 244 ),
		'Currency-due' => array( 244, 248 ),
		'Amount-due' => array( 248, 260 ),
		'Amount-sign_amount' => array( 260, 261 ),
		'Date-due' => array( 261, 269 ),
		'Authorization-indicator' => array( 269, 270 ),
		'Filler' => array( 270, 320 ),
		'Currency-original' => array( 320, 324 ),
		'Amount-original' => array( 324, 336 ),
		'Amount-sign-original' => array( 336, 337 ),
		'Reserved_7' => array( 337, 349 ),
		'Order-number-original' => array( 349, 389 ),
		'Customer-ID-original' => array( 389, 404 ),
	);
	
	/**
	 * Get local field position map
	 */
	public function getLocalFieldPositionMap() {
		return $this->getRefundCreditCardFieldPositionMap();
	}
	
	public function getRefundCreditCardFieldPositionMap() {
		return $this->refundCreditCardFieldPositionMap;
	}
}
