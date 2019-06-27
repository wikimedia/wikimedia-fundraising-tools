#!/usr/bin/php
<?php
/**
 * Note: this script expects any export to happen within the --year passed so it can work out of days of that year.
 * It will get confused if the dates passed span across different years.
 *
 * Sample usage:
 * ./exchange_rates_export --from "2018-09-02" --to "2018-12-31" --year "2018" --filename "2018"
 * ./exchange_rates_export --from "2019-01-01" --year "2019" --filename "2019"
 */

// set up the cli args
$opts = getopt("", ['from:', 'to:', 'year:', 'filename:']);
$from = isset($opts['from']) ? new DateTime($opts['from']) : new DateTime("2019-01-01");
$to = isset($opts['to']) ? new DateTime($opts['to']) : new DateTime();
$year = isset($opts['year']) ? $opts['year'] : '2018';
$file = isset($opts['filename']) ? $opts['filename'] : 'exchange_rates';
$daysDiff = $from->diff($to)->format("%a");

// set up document array and add headers
$doc = [
    //set headers
    array_merge(['Year'], array_fill_keys(range(1, $daysDiff + 1), $year)),
    // php day-of-the-year output begins at 0 so we bump it up 1 so that the csv days start from 1.
    array_merge(['Day of year'], range($from->format("z")+1, $from->format("z") + $daysDiff + 1))];

// loop through currencies list, pull in rates data between $from and $to, build out the csv $row with the results.
$exchangeRates = new ExchangeRates();
$currencies = $exchangeRates->getAllCurrencies();
foreach ($currencies as $currency) {

    $rates = $exchangeRates->getAllRatesForCurrency($currency, $from->format("Y-m-d"), $to->format("Y-m-d"));

    // we add the rates data to an array that represents a row in the CSV. more specifically we add
    // the columns first and then the data second so we don't lose any CSV row cells due to missing data
    $columns = array_fill_keys(range($from->format("z"), $from->format("z") + $daysDiff), null);
    $row = $rates + $columns;
    // fix the row ordering
    ksort($row);

    //loop through to find any empty cells and populate with the previous' days rate if needed
    array_walk($row, function (&$value, $key) use ($row) {
        if ($value === null) {
            // pull the closet with value
            if ($row[$key - 1] === null) {
                $value = $row[$key - 2];
            } else {
                $value = $row[$key - 1];
            }
        }
    });

    //add currency ISO Code as first column
    array_unshift($row, $currency);

    //add row to document array
    $doc[] = $row;
}

// write the document array to csv
$file = fopen($file . ".csv", 'w');
foreach ($doc as $line) {
    fputcsv($file, $line);
}
fclose($file);

class ExchangeRates
{

    const MYSQL_CONF = ".my.cnf";
    const DB = "drupal";
    const DB_TABLE = "exchange_rates";

    protected $adapter;

    /**
     * ExchangeRates constructor.
     *
     * mysql db config file should live in each users home dir
     * on frdev1001 and be called '.my.cnf'. If this changes,
     * speak to fr-tech-ops.
     *
     */
    public function __construct()
    {
        $cfg = parse_ini_file(self::MYSQL_CONF, true, INI_SCANNER_RAW);
        $db = self::DB;
        $this->adapter =
            new PDO("mysql:host={$cfg['mysql']['host']};dbname={$db}", $cfg['mysql']['user'],
                $cfg['mysql']['password']);
    }

    /**
     * Get a full list of currencies we have rates for
     * @return array
     */
    public function getAllCurrencies()
    {
        $tbl = self::DB_TABLE;
        $sql = "SELECT DISTINCT currency FROM $tbl";
        $result = $this->adapter->query($sql)->fetchAll(PDO::FETCH_COLUMN);
        return $result;
    }

    /**
     * Fetch all rates for a given currency between $from and $to.
     * Also pull in the day of the year the rate was provided so we can line that
     * up with our CSV columns.
     *
     * Note: we adjust the MYSQL dayofyear key values down 1 to match PHP's convention.
     * MYSQL starts from 1 and PHP from 0...
     *
     * @param $currency
     * @param $from
     * @param $to
     * @return array
     */
    public function getAllRatesForCurrency($currency, $from, $to)
    {
        $tbl = self::DB_TABLE;
        $sql = "SELECT DAYOFYEAR(FROM_UNIXTIME(bank_update))-1, value_in_usd FROM $tbl
              WHERE currency = :currency
              AND FROM_UNIXTIME(bank_update) >= '$from'
              AND FROM_UNIXTIME(bank_update) <= '$to'
              ORDER BY bank_update ASC";
        $stmt = $this->adapter->prepare($sql);
        $stmt->execute([':currency' => $currency]);
        $result = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        return $result;
    }
}
