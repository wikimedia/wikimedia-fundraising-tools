<?php 
require_once( 'Wr1DataRecord_CreditCard.php' );

/**
 * Parsing class for "-CB" records, which are chargebacks on previous 
 * +ON records
 */
class Wr1DataRecord_CreditCard_Chargeback extends Wr1DataRecord_CreditCard {
	
	/**
	 * @see Wr1DataRecord::fieldPositionMap
	 */
	private $chargebackCreditCardFieldPositionMap = array(
		'Record-category' => array( 0, 1 ),
		'Record-type' => array( 1, 3 ),
		'Reserved_0' => array( 3, 15 ),
		'Order-number' => array( 15, 45 ),
		'Customer-ID' => array( 45, 60 ),
		'Reserved_1' => array( 60, 167 ),
		'Card-number' => array( 168, 186 ),
		'Reserved_2' => array( 186, 187 ),
		'Payment-processor-id' => array( 187, 190 ),
		'Expiry-date' => array( 190, 194 ),
		'Issue-number' => array( 194, 196 ),
		'Reserved_3' => array( 196, 208 ),
		'WbC Payment method ID' => array( 208, 210 ),
		'WbC Paymentproduct ID' => array( 210, 214 ),
		'Reserved_4' => array( 214, 220 ),
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
		'Reserved_5' => array( 269, 320 ),
		'Currency-original' => array( 320, 324 ),
		'Amount-original' => array( 324, 336 ),
		'Amount-sign-original' => array( 336, 337 ),
		'Reserved_6' => array( 337, 349 ),
		'Order-number-original' => array( 349, 369 ),
		'Customer-ID-original' => array( 369, 384 ),
		'Chargeback-reason-ID' => array( 384, 386 ),
		'Chargeback-reason-desc' => array( 386, 411 ),
		'Date-collect-original' => array( 411, 419 ),
	);
	
	/**
	 * Get local field position map
	 */
	public function getLocalFieldPositionMap() {
		return $this->getChargebackCreditCardFieldPositionMap();
	}
	
	public function getChargebackCreditCardFieldPositionMap() {
		return $this->chargebackCreditCardFieldPositionMap;
	}
}
