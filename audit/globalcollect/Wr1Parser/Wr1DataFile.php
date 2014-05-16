<?php 

/**
 * Fetches an iterable object which on each iteration returns a valid and 
 * parsed data record from a Wr1 file.
 */
class Wr1DataFileParser {
	
	public function __construct( $filePath ) {
		$this->wr1DataFileObject = new Wr1DataFile_Iterator( $filePath );
	}
	
	/**
	 * Fetches an iterable object of data records from a wr1 file
	 */
	public function getRecordIterator() {
		$fileReader = new Wr1DataFile_Iterator_Filter( $this->wr1DataFileObject );
		return $fileReader;
	}
}

/**
 * Iterable Wr1 parser
 * 
 * This allows for nice wr1 file handling. It opens/closes the file and is the
 * base from which we interact with the wr1. You can define which record types
 * this suite of classes can handle and either parse/process the wr1 all at
 * once (which is not recommended for large wr1 files, otherwise you will run
 * out of memory), or iterate over an instance of this class to process the
 * file line-by-line. For a nice, filtered version of this iterator, use 
 * Wr1DataFileParser::getRecordIterator().
 * 
 * @TODO it would be cool to define parsing classes for the header and trailer
 * records, then load just those data objects into properites of an instance of
 * this class. Those records are really just meta information, but may be
 * useful at some point to be accessible from here.
 */
class Wr1DataFile_Iterator implements Iterator {
		
	/**
	 * Map of data record type => data handling class
	 * 
	 * The record type is defined in the first 3 chars of the data record:
	 *   <1 char record category><2 char record type>
	 * Details of which can be found in the GlobalCollect WDL programmer guide
	 * for reporting.
	 * @var array
	 */
	protected $recordTypeMap = array(
		'XON' => 'Wr1DataRecord_CreditCard_Processed',
		'+IP' => 'Wr1DataRecord_InvoicePayment_Settled',
		'-CB' => 'Wr1DataRecord_CreditCard_Chargeback',
		'-CR' => 'Wr1DataRecord_CreditCard_Refund',
	);
	
	/** 
	 * Full path to wr1 file 
	 * @var string
	 */
	protected $filePath = null;

	/**
	 * Header records from wr1 file 
	 * @var array
	 */
	protected $headerRecords = array();
	
	/**
	 * Data records from wr1 file 
	 * @var array
	 */
	protected $dataRecords = array();	
	
	/**
	 * Trailer records from wr1 file 
	 * @var array
	 */
	protected $trailerRecords = array();
	
	/**
	 * The current record in the process of iteration
	 * 
	 * When using the iteration feature of this object, the current record from
	 * the wr1 file is stored here.
	 * @var object
	 */
	protected $currentRecord;
	
	/**
	 * The number of records processed so far
	 * @var int
	 */
	protected $recordCount = 0;
	
	/**
	 * File handler for wr1 file
	 * @var resource
	 */
	protected $fh;
	
	public function __construct( $filePath ) {
		$this->setFilePath( $filePath );
		// open file
		$this->fh = fopen( $this->getFilePath(), 'r' );
		if ( $this->fh === false ) {
			throw new Exception( "Failed to open '{$this->getFilePath()}'" );
		}
	}
	
	/**
	 * This will parse the entire wr1 file and all its data records
	 * 
	 * It turns the data records into corresponding objects and makes them
	 * locally accessible. This should be used with extreme caution as it 
	 * will easily and happily devour all the memory you have allocated
	 * to PHP given a large enough wr1 file.
	 */
	public function parse() {
		// according to the docs, line length will not exceed 420b
		while( $line = fgets( $this->fh, 420 )) {
			$record = $this->processRecord( $line );
			$recordType = $record[0];
			$recordObject = $record[1];
			
			// deal with the appropriate record type
			switch ( $recordType ) {
				// header records
				case 'IFH':
				case 'IBH':
					array_push( $this->headerRecords, $recordObject );
					break;
					
				// trailer records	
				case 'IBT':
				case 'IFT':
					array_push( $this->trailerRecords, $recordObject );
					break;
	
				// information records
				case 'ITM':
					array_push( $this->informationRecords, $recordObject );
					break;
						
				// regular data records
				default:
					array_push( $this->dataRecords, $recordObject );
					break;
			}
			++$this->recordCount;
			//echo memory_get_usage() . "\n";
		}
	}
	
	/**
	 * Process a record
	 * 
	 * @TODO error/log/? on non-matching record type
	 * @param string $record Record line from wr1 file.
	 * @return array array[0] is the record type, array[1] is the record object
	 */
	public function processRecord( $record ) {
		// figure out record type
		$recordType = $this->determineRecordType( $record );
		
		// If the record type is not in record type map, we can't handle it.
		if ( !isset( $this->recordTypeMap[ $recordType ] )) {
			// For now, do nothing. Perhaps error? log?
			return false;
		}
		
		// determine the class which will handle this data record type
		$recordClass = $this->recordTypeMap[ $recordType ];
		
		// instantiate an object for this data record type
		if ( !class_exists( $recordClass ) ) {
			require_once( "$recordClass.php" ); //:p
		}
		$recordObject = new $recordClass( $record );
		
		// parse the mofo.
		$recordObject->parse();
		
		return array( $recordType, get_object_vars( $recordObject ));
	}
	
	public function getFilePath() {
		return $this->filePath;
	}
	
	/**
	 * Set the file path for the wr1 file.
	 * 
	 * Should be full path to the file.
	 * @TODO clean up and ensure existence of file.
	 * @param string $filePath
	 */
	public function setFilePath( $filePath ) {
		// clean file name
		// check file existence
		$this->filePath = $filePath;
	}
	
	public function getRecordTypeMap() {
		return $this->recordTypeMap;
	}
	
	/**
	 * Setter for $this->recordTypeMap
	 * @TODO check that param is an array; ensure class actually exists
	 */
	public function setRecordTypeMap( $recordTypeMap ) {
		// check for array
		// check for existence of classes
		$this->recordTypeMap = $recordTypeMap;
	}
	
	/**
	 * Determine the record type from a wr1 record line
	 * 
	 * This is determinable by grabbing the first three chars of the line.
	 * @param string $line
	 * @return string
	 */
	public function determineRecordType( $line ) {
		$recordType = substr( $line, 0, 3 );
		return $recordType;
	}
	
	public function getDataRecords() {
		return $this->dataRecords;
	}
	
	/**
	 * Begin iterator methods
	 */

	/**
	 * Returns $this->currentRecord
	 * @see Iterator::current()
	 */
	public function current() {
		return $this->getCurrentRecord();
	}

	/**
	 * Returns the number of records processed so far
	 * @see Iterator::key()
	 */
	public function key() {
		return $this->recordCount;
	}
	
	/**
	 * Sets $this->currentRecord() to the next record
	 * @see Iterator::next()
	 */
	public function next() {
		$this->setCurrentRecord();
	}
	
	/**
	 * Reset the file pointer to the beginning and load the first record
	 * @see Iterator::rewind()
	 */
	public function rewind() {
		fseek( $this->fh, 0 );
		$this->setCurrentRecord();
	}
	
	/**
	 * Ensure that we have not yet hit the end of the wr1 file
	 * 
	 * If we have, the iterator is no longer valid since there's noting left
	 * to process.
	 * 
	 * @see Iterator::valid()
	 */
	public function valid() {
		$isValid = ( !feof( $this->fh ));
		return $isValid;
	}
	
	/**
	 * Fetch the next record from the file
	 * 
	 * In the event that there is actually a next record in the file, advance 
	 * the record counter.
	 * @return string|false
	 */
	public function getNextRecord() {
		$record = fgets( $this->fh, 420 );
		if ( $record ) {
			$recordDetails = $this->processRecord( $record );
			if ( is_array( $recordDetails )) {
				$record = $recordDetails[1];
				++$this->recordCount;
			} else {
				return false;
			} 
		}
		return $record;
	}
	
	/**
	 * End iterator methods
	 */
	
	/**
	 * Set the current record object
	 * 
	 * If no record is passed in to set, advance to the next record in the wr1
	 * file and set that to current.
	 * @param object
	 */
	public function setCurrentRecord( $record = null ) {
		if ( is_null( $record )) {
			$this->currentRecord = $this->getNextRecord();
		} else {
			$this->currentRecord = $record;
		}
	}
	
	/**
	 * Getter for $this->currentRecord
	 */
	public function getCurrentRecord() {
		return $this->currentRecord;
	}
	
	public function __destruct() {
		// if the file handler is still open, close it.
		if ( gettype( $this->fh == 'resource' )) {
			fclose( $this->fh );
		}
	}
}

/**
 * A filter iterator for the Wr1DataFile_Iterator
 * 
 * This class allows us to define records to ignore during iteration. Because
 * we only want to be returning records for which we have a parsing/processing
 * class, this will limit iterable output to records which have actually been
 * parsed/processed.
 */
class Wr1DataFile_Iterator_Filter extends FilterIterator {
	public function __construct( Wr1DataFile_Iterator $iterator ) {
		parent::__construct( $iterator );
	}
	
	/**
	 * Return only records which have been processed.
	 * 
	 * Records which have not parsed/processed are stored as (bool) false, so
	 * if that is the current item, do not accept it and move on to the next.
	 * 
	 * This is a required method of a FilterIterator.
	 * @see FilterIterator::accept()
	 */
	public function accept() {
		return ( parent::current() === false ) ? false : true;
	}
}

