<?php

/**
 * Contains both the metadata and content of a report response
 */
class ReportResponse {

	/* Protected Members */
	protected $reportId = array();
	protected $numberOfColumns = array();
	protected $numberOfRows = array();
	protected $numberOfPages = array();
	protected $pageSize = array();
	protected $iterations = -1;
	
	/* Public Members */
	/**
	 * Stores column data object
	 * @var reportColumnData object
	 */
	public $columns;
	public $rows = array();
	
	public function __construct( $xml_metadata=null ) {
		if ( !is_null( $xml_metadata )) {
			$this->parseMetaData( $xml_metadata );
		}		
	}

	/**
	 * Parse metadata
	 * 
	 * Locally store meta-metadata (rows, pages, etc) and turn column metadata
	 * into a local array.
	 * @param SimpleXMLElement $xml
	 */
	public function parseMetaData( SimpleXMLElement $xml ) {
		// increment our count of how many times we've handled report meta data
		$this->iterations++;
		
		array_push( $this->numberOfColumns, $xml->getMetaDataResponse->numberOfColumns );
		array_push( $this->numberOfRows, $xml->getMetaDataResponse->numberOfRows );
		array_push( $this->numberOfPages, $xml->getMetaDataResponse->numberOfPages );
		array_push( $this->reportId, $xml->getMetaDataResponse->reportId );
		array_push( $this->pageSize, $xml->getMetaDataResponse->pageSize );

		// @TODO this can likely be handled better
		if ( $this->iterations && $this->numberOfColumns[ $this->iterations - 1 ] != $this->numberOfColumns[ $this->iterations ] ) {
			//we have a problem as our metadata columns are out of sync
			die( 'Metadata has changed between report runs!' );
		}
		
		// if this is the first time we're running the meta data parser, parse the column data
		if ( !$this->iterations ) {
			$columnData = new reportColumnData();
			$columnData->parseColumnData( $xml->getMetaDataResponse->columnMetaData );
			$this->columns = $columnData;
		}
	}
	
	/**
	 * Add the actual 'data' portion of the Payflow report
	 * 
	 * Parses data rows and stores them as reportDataRow objects in $this->rows
	 * These objects are iterable and filterable
	 * @param SimpleXMLElement $xml
	 */
	public function addData( SimpleXMLElement $xml ) {
		foreach ( $xml->getDataResponse->reportDataRow as $row ) {
			$reportDataRow = new reportDataRow( $row );
			array_push( $this->rows, $reportDataRow); 
		}
	}
	
	/**
	 * Return the column number for a given param
	 * 
	 * @param mixed $param PayFlow paramter name or array of names
	 * @return mixed Will return array if $param is an array, int or false if $param is a string
	 */
	public function findColumnNumber( $param ) {
		if ( is_array( $param ) ) {
			$ret = array();
			foreach( $param as $val ) {
				$ret[ $val ] = $this->_findColumnNumber( $val );
			}
		} elseif( is_string( $param )) {
			$ret = $this->_findColumnNumber( $param );
		} else {
			throw new InvalidArgumentException( 'String or array expected as parameter.' );
		}
		
		return $ret;
	}
	
	/**
	 * Helper function for public function findColumnNumber()
	 * @param string $param
	 */
	protected function _findColumnNumber( $param ) {
		if ( !is_string( $param )) {
			throw new InvalidArgumentException( 'String expected.' );
		}
		return array_search( $param, $this->columns->columns );
	}
	
	/**
	 * Return the column name for a given column number
	 * @param mixed $colNum either a single column number or an array of column numbers
	 * @return string or false
	 */
	public function findColumnName( $colNum ) {
		if ( is_array( $colNum )) {
			$ret = array();
			foreach ( $colNum as $val ) {
				array_push( $ret, $this->_findColumnName( $val ));
			}
			return $ret;
		} else {
			return $this->_findColumnName( $colNum );
		}
	}
	
	/**
	 * Helper function for public function findColumnName
	 * @param int $colNum
	 */
	protected function _findColumnName( $colNum ) {
		if ( !is_numeric( $colNum )) {
			throw new InvalidArgumentException( 'Numeric parameter expected.' );
		}
		return ( $this->columns->columns[$colNum] ) ? $this->columns->columns[$colNum] : false;
	}
	
	/**
	 * Get the number of pages for a given iteration of report metadata
	 * 
	 * If $iteration is not supplied, assumes current iteration.
	 * @param int $iteration
	 * @throws InvalidArgumentException
	 * @return int
	 */
	public function getNumberOfPages( $iteration=null ) {
		if ( is_null( $iteration ) ) {
			$iteration = $this->iterations;
		}
		
		if ( !is_numeric( $iteration )) {
			throw new InvalidArgumentException( 'Numeric parameter expected.' );
		}
		
		return $this->numberOfPages[ $iteration ];
	}
}

/**
 * An iterable object containing column data taken from the metadata response
 *
 * Can be given an array of valid 'keys' that, when present, will only
 * return corresponding data valude during iteration.
 */
class reportColumnData implements IteratorAggregate {
	/**
	 * An array of data keys allowed to be returned on iteration
	 * @var array or null
	 */
	protected $filter_keys = null;
	
	/**
	 * The object containing column data
	 * @var SimpleXMLElement
	 */
	protected $columnXml;
	
	/**
	 * An array holding column data
	 * @var array
	 */
	public $columns = array();
	
	/**
	 * Allow this object to be iterable on $this->data
	 * 
	 * If there are values set in $this->filter_keys, only items with
	 * corresponding keys in $this->data will be available in the iterator.
	 */
	public function getIterator() {
		return new KeyFilterIterator( new ArrayIterator( $this->columns ), $this->filter_keys );
	}
	
	/**
	 * Parse column data from SimpleXMLElement object into an array
	 * 
	 * $this->columns[ <column number> ] = <column data>
	 * @param SimpleXMLElement $columnMetaData
	 */
	public function parseColumnData( SimpleXMLElement $columnMetaData ) {
		// determine the column names and corresponding column numbers
		foreach ( $columnMetaData as $columnData ) {
			$colNum = (int) $columnData['colNum'];
			$this->columns[ $colNum ] = (string) $columnData->dataName;
		}
	}
	
	public function setFilterKeys( array $filter_keys ) {
		if ( count( $filter_keys )) {
			$this->filter_keys = $filter_keys;
		}
	}
	
	public function getFilterKeys() {
		return $this->filter_keys;
	}
}

/**
 * An iterable object containing data found in a report row
 * 
 * Can be given an array of valid 'keys' that, when present, will only 
 * return corresponding data values during iteration.
 */
class reportDataRow implements IteratorAggregate {
	/**
	 * Row number of row in report data
	 * @var int
	 */
	public $rowNum;
	
	/**
	 * Array containing actual data points in the row
	 * Key is 'column number' and value is the data value
	 * @var array
	 */
	public $data = array();
	
	/**
	 * An array of data keys allowed to be returned on iteration
	 * @var array or null
	 */
	protected $filter_keys = null;
	
	/**
	 * The object containing row data
	 * @var SimpleXMLElement
	 */
	protected $rowXml;
	
	public function __construct( SimpleXMLElement $xml ) {
		// store the raw XML - this may come in handy for debug
		$this->rowXml = $xml; 
		$this->rowNum = (int) $xml['rowNum'];
		$this->parseDataRow( $xml );
	}
	
	/**
	 * Parse the data in the row
	 * 
	 * Stores individual datum in $this->data, which is used when iterating
	 * $this or can be accessed stand-alone.
	 */
	public function parseDataRow( SimpleXMLElement $rowXml ) { 
		foreach ( $rowXml->columnData as $columnData ) {
			$colNum = (int)  $columnData['colNum'];
			$this->data[ $colNum ] = (string) $columnData->data;
		}
	}
	
	/**
	 * Allow this object to be iterable on $this->data
	 * 
	 * If there are values set in $this->filter_keys, only items with
	 * corresponding keys in $this->data will be available in the iterator.
	 */
	public function getIterator() {
		return new KeyFilterIterator( new ArrayIterator( $this->data ), $this->filter_keys );
	}
	
	public function setFilterKeys( array $filter_keys ) {
		if ( count( $filter_keys )) {
			$this->filter_keys = $filter_keys;
		}
	}
	
	public function getFilterKeys() {
		return $this->filter_keys;
	}
}

/**
 * Filter items returned by iterator based on a list of keys
 */
class KeyFilterIterator extends FilterIterator {
	public $valid_keys = null;
	
	public function __construct( Iterator $iterator, $valid_keys=null ) {
		if ( $valid_keys && !is_array( $valid_keys )) {
			//we have a problem
			throw new InvalidArgumentException( 'Null or array expected as parameter.' );
		}
		$this->valid_keys = $valid_keys;
		parent::__construct( $iterator );
	}
	
	/**
	 * Check to see if $this->current() (value of the current item in the iterator) 
	 * ought to be returned.
	 * 
	 * If filter keys are NOT defined, we'll always return the current item in
	 * the iterator.  If filter keys are defined, we check to see if the current
	 * item's key is in the list of valid keys.  If so, we return it - if not, we
	 * don't.
	 * 
	 * @see FilterIterator::accept()
	 * @return bool
	 */
	public function accept() {
		// if no keys are defined, assume all keys are valid
		if ( is_null( $this->valid_keys )) {
			return true;
		}
		
		// otherwise, check if the key is in the list of valid keys.
		return in_array( $this->key(), $this->valid_keys );
	}
}

?>
