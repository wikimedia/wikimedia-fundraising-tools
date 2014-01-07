<?php
require_once( 'Wr1DataFile.php' );
abstract class Wr1DataRecord {

	/**
	 * Raw data record taken from a line in the wr1 file 
	 * @var string
	 */
	protected $rawRecord;
	
	/**
	 * Definition of the fields and their positions in the wr1 file
	 * 
	 * These can be used to chunk the raw record string into its constituent
	 * fields using something like substr(). This map is in the format:
	 *   array( 'field name' => array( start position, end position ))
	 * Note that the 'start position' will be 1 less than the start position
	 * defined in the WDL programmer guide in order to work correctly with
	 * substr().
	 * 
	 * Certain field names in the programmer guide get repeated multiple times
	 * for a given data record, like 'Amount-sign', 'Reserved', 'Filler', which
	 * we deal with in a novel way that shoudl remain consistent throughout
	 * data record child classes.
	 * 	'Reserved' is represented as 'Reserved_0', 'Reserved_1', etc in numeric
	 * 		order of apperance.
	 * 	'Filler' follows the same logic as 'Reserved'.
	 * 	'Amount-sign' becomes 'Amount-sign_<amount description>'. For instance,
	 * 		if 'Amount-sign' follows the 'Payment-amount' field, we turn it
	 * 		into 'Amount-sign_payment'.
	 * 
	 * Taken from pp23 of the GlobalCollect WDL Programmers Guide - Reporting
	 * from 16 July 2009.
	 * @var unknown_type
	 */
	protected $fieldPositionMap = array();

	/**
	 * Instantiate the data record object
	 * 
	 * Upon instantiation, the fieldPositionMap will be set for the specific
	 * data record type.
	 */
	public function __construct( $rawRecord ) {
		$this->setFieldPositionMap();
		$this->setRawRecord( $rawRecord );
	}
	
	/**
	 * Set field position map
	 * 
	 * In the event that the fieldPositionMap is not explicitly passed in, this
	 * will attempt to laod the specific data record type's local field
	 * position map. 
	 * 
	 * @param array $fieldPositionMap
	 * @throws InvalidArgumentException
	 */
	public function setFieldPositionMap( $fieldPositionMap = null ) {
		if ( is_null( $fieldPositionMap )) {
			$fieldPositionMap = $this->getLocalFieldPositionMap();
		}
		
		if ( !is_array( $fieldPositionMap )) {
			throw new InvalidArgumentException( 
				'setFieldPositionMap() expects an array.' );
		}
		
		$this->fieldPositionMap = $fieldPositionMap;
	}
	
	/**
	 * Return the field position map
	 * @return array
	 */
	public function getFieldPositionMap() {
		return $this->fieldPositionMap;
	}
	
	/**
	 * Fetch the 'local' field position map
	 * 
	 * 'Local' field positin map refers to the position map unique to the 
	 * specific data record type. Each data record type needs to define this 
	 * method.
	 * 
	 * @see $this->fieldPositionMap for fieldPositionMap informtaion.
	 * @return array
	 */
	abstract public function getLocalFieldPositionMap();
	
	/**
	 * Parse the data record.
	 * 
	 * This will turn each field from the data record, as defined in the record
	 * map, into a public local object property.
	 * 
	 * This means the object will be iterable over these properties, and the
	 * fields can also be determined with get_object_vars()
	 */
	public function parse() {
		foreach ( $this->fieldPositionMap as $field => $positions ) {
			$this->$field = trim( substr( $this->rawRecord, $positions[0], ( $positions[1] - $positions[0] )));
		}
	}
	
	/**
	 * Set the raw data record
	 *
	 * This is essentially the raw record line from the wr1 file.
	 * @param string $rawRecord
	 */
	public function setRawRecord( $rawRecord ) {
		$this->rawRecord = $rawRecord;
	}
	
	/**
	 * Return the raw data record
	 * @return string
	 */
	public function getRawRecord() {
		return $this->rawRecord;
	}
}
