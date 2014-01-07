<?php error_reporting (E_ALL);

require_once( 'PayflowReports.php' );

/*
 * Each PayflowReportQuery subclass needs to implement runReportRequest() which uses arguments 
 * created from the constructor
 *
 */

class DailyActivityReport extends PayflowReportQuery {
	public $reportName = 'DailyActivityReport';
	protected $reqd_options = array( 'report_date' );
	
	public function __construct( $date = null ) {
		parent::__construct( $date );
		
		$options = array( 
			'report_date' => date( 'Y-m-d', strtotime( $this->date['start'] ) ),
			'pageSize'    => $this->pageSize,
		);
		$this->setOptions( $options );
	}
}

class CustomReport extends PayflowReportQuery {
	public $reportName = 'CustomReport';
	protected $reqd_options = array( 'start_date', 'end_date' );	
	
	/**
	 * An array containing the default options for this report type.
	 * @var array
	 */
	protected $defaultOptions = array();
	
	public function __construct( $date = null ) {
		parent::__construct( $date );
		$this->loadAuthData();
		
		$options = array(
			'start_date' => $this->date['start'],
			'end_date'   => $this->date['end'],
			'pageSize'   => $this->pageSize,
			'results'    => 'Approvals Only',
			'sort_by'    => 'Transaction Time',
			'timezone'   => 'GMT',
		);
		$this->defaultOptions = $options;
		$this->setOptions( $options );
	}
}

class FilterScorecardReport extends PayflowReportQuery {
	private $reportName = 'FilterScorecardReport';
	protected $reqd_options = array( 'start_date', 'end_date' );
	
	public function __construct( $date = null, $tz = null ) {
		parent::__construct( $date );
		$this->tz = ( isset(  $tz ) ) ? $tz : 'GMT';
		
		$this->setOptions = array( 
			'start_date' => $this->date['start'],
			'end_date' => $this->date['end'],
			'timezone' => $this->tz,
		);
	}
}

?>
