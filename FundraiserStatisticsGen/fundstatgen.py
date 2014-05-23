#!/usr/bin/python

"""
Fundraiser Statistics Generation

Queries the CiviCRM database to
"""

import MySQLdb as db
import csv
import json
from optparse import OptionParser
from ConfigParser import SafeConfigParser
import logging
from logging.handlers import SysLogHandler

CONFIG_DEFAULT_PATH = "/etc/fundraising/fundstatgen.cfg"

def main():
    # Extract any command line options
    parser = OptionParser(usage="usage: %prog [options] <working directory>")
    parser.add_option("-c", "--config", dest='configFile', default=None, help='Path to configuration file')
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        exit(1)
    workingDir = args[0]

    # Load the configuration from the file
    config = SafeConfigParser()
    fileList = [CONFIG_DEFAULT_PATH]
    if options.configFile:
        fileList.append(options.configFile)
    config.read(fileList)

    # === BEGIN PROCESSING ===
    hostname = config.get('MySQL', 'hostname')
    port = config.getint('MySQL', 'port')
    username = config.get('MySQL', 'username')
    password = config.get('MySQL', 'password')
    database = config.get('MySQL', 'schema')

    logging.debug("Will connect to MySQL using: %s@%s:%d", username, hostname, port)

    logging.info("Running per year query...")
    stats = getPerYearData(hostname, port, username, password, database)

    logging.info("Pivoting data into year/day form...")
    (years, pivot) = pivotDataByYear(stats)

    logging.info("Writing year data output files...")
    createSingleOutFile(stats, 'date', workingDir + '/donationdata-vs-day')
    createOutputFiles(pivot, 'date', workingDir + '/yeardata-day-vs-', years)

    logging.info("Running per campaign query...")
    pcStats = getPerCampaignData(hostname, port, username, password, database)

    logging.info("Writing campaign data output files...")
    createSingleOutFile(pcStats, ('medium', 'campaign'), workingDir + '/campaign-vs-amount')


def getPerYearData(host, port, username, password, database):
    """
    Obtain basic statistics (USD sum, number donations, USD avg amount, USD max amount,
    USD YTD sum) per day from the MySQL server.

    Returns a dict like: {date => {report type => {value}} where report types are:
    - sum, refund_sum, donations, refunds, avg, max, ytdsum, ytdloss
    """
    con = db.connect(host=host, port=port, user=username, passwd=password, db=database)
    cur = con.cursor()
    cur.execute("""
        SELECT
          DATE_FORMAT(con.receive_date, "%Y-%m-%d") as receive_date,
          SUM(IF(con.total_amount >= 0, total_amount, 0)) as credit,
          SUM(IF(con.total_amount >= 0, 1, 0)) as credit_count,
          SUM(IF(con.total_amount < 0, total_amount, 0)) as refund,
          SUM(IF(con.total_amount < 0, 1, 0)) as refund_count,
          AVG(IF(con.total_amount >= 0, total_amount, 0)) as `avg`,
          MAX(con.total_amount)
        FROM civicrm_contribution con, drupal.contribution_tracking ct
        WHERE
          con.id=ct.contribution_id AND
          receive_date >= '2006-01-01'
        GROUP BY DATE_FORMAT(con.receive_date, "%Y-%m-%d") ASC;
        """)

    data = {}
    ytdCreditSum = 0
    ytdRefundSum = 0
    cyear = 0
    for row in cur:
        (date, credit_sum, credit_count, refund_sum, refund_count, avg, max) = row
        year = int(date[0:4])
        credit_sum = float(credit_sum)
        credit_count = int(credit_count)
        refund_sum = float(refund_sum)
        refund_count = int(refund_count)
        avg = float(avg)
        max = float(max)

        if cyear != year:
            cyear = year
            ytdCreditSum = 0
            ytdRefundSum = 0
        ytdCreditSum += credit_sum
        ytdRefundSum += refund_sum

        data[date] = {
            'sum': credit_sum,
            'refund_sum': refund_sum,
            'donations': credit_count,
            'refunds': refund_count,
            'avg': avg,
            'max': max,
            'ytdsum': ytdCreditSum,
            'ytdloss': ytdRefundSum
        }

    del cur
    con.close()
    return data


def getPerCampaignData(host, port, username, password, database):
    """
    Obtain basic statistics (USD sum, number donations, USD avg amount, USD max amount,
    USD YTD sum) per medium, campaign

    Returns a dict like: {(medium, campaign) => {value => value} where value types are:
    - start_date, stop_date, count, sum, avg, std, max
    """
    con = db.connect(host=host, port=port, user=username, passwd=password, db=database)
    cur = con.cursor()
    cur.execute("""
        SELECT
          ct.utm_medium,
          ct.utm_campaign,
          min(c.receive_date),
          max(c.receive_date),
          count(*),
          sum(c.total_amount),
          avg(c.total_amount),
          std(c.total_amount),
          max(c.total_amount)
        FROM drupal.contribution_tracking ct, civicrm.civicrm_contribution c
        WHERE
          ct.contribution_id=c.id AND
          c.total_amount >= 0
        GROUP BY utm_medium, utm_campaign;
        """)

    data = {}
    for row in cur:
        (medium, campaign, start, stop, count, sum, usdavg, usdstd, usdmax) = row
        count = int(count)
        sum = float(sum)
        usdavg = float(usdavg)
        usdstd = float(usdstd)
        usdmax = float(usdmax)

        data[(medium, campaign)] = {
            'start_date': start.isoformat(),
            'stop_date': stop.isoformat(),
            'count': count,
            'sum': sum,
            'avg': usdavg,
            'usdstd': usdstd,
            'usdmax': usdmax
        }

    del cur
    con.close()
    return data


def pivotDataByYear(stats):
    """
    Transformation of the statistical data -- grouping reports by date

    Returns ((list of years), {report: {date: [year data]}})
    """
    years = []
    pivot = {}

    reports = stats.values()[0].keys()
    for report in reports:
        pivot[report] = {}

    # Do the initial pivot
    for date in stats:
        (year, month, day) = date.split('-')
        if year not in years:
            years.append(year)

        for report in reports:
            if ('2000/%s/%s 23:59:59' % (month, day)) not in pivot[report]:
                pivot[report]['2000/%s/%s 23:59:59' % (month, day)] = {}
            pivot[report]['2000/%s/%s 23:59:59' % (month, day)][year] = stats[date][report]

    # Now listify the data
    years.sort()
    for report in reports:
        for linedate in pivot[report]:
            newline = []
            linedata = pivot[report][linedate]
            for year in years:
                if year in linedata:
                    newline.append(linedata[year])
                else:
                    newline.append(None)
            pivot[report][linedate] = newline

    return years, pivot


def createOutputFiles(stats, firstcol, basename, colnames = None):
    """
    Creates a CSV file for each report in stats
    """
    reports = stats.keys()
    for report in reports:
        createSingleOutFile(stats[report], firstcol, basename + report, colnames)


def createSingleOutFile(stats, firstcols, basefilename, colnames = None):
    """
    Creates a single report file from a keyed dict

    stats       must be a dictionary of something list like; if internally it is a dictionary
                then the column names will be taken from the dict; otherwise they come colnames

    firstcols   can be a string or a list depending on how the data is done but it should
                reflect the primary key of stats
    """
    if colnames is None:
        colnames = stats.itervalues().next().keys()
        colindices = colnames
    else:
        colindices = range(0, len(colnames))

    if isinstance(firstcols, basestring):
        firstcols = [firstcols]
    else:
        firstcols = list(firstcols)

    csvfilename = basefilename + ".csv"
    f = file(csvfilename, 'w')
    csvf = csv.writer(f)
    csvf.writerow(firstcols + colnames)

    alldata = []
    for linekey in sorted(stats.keys()):
        if isinstance(linekey, basestring):
            linekeyl = [linekey]
        else:
            linekeyl = list(linekey)

        rowdata = [stats[linekey][col] for col in colindices]
        alldata.append(linekeyl + rowdata)
        csvf.writerow(linekeyl + rowdata)
    f.close()

    jsonfilename = basefilename + ".json"
    f = file(jsonfilename, 'w')
    mapstats = [ dict(zip(firstcols + colnames, line)) for line in alldata ]
    json.dump(mapstats, f)
    f.close()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    try:
        logging.captureWarnings(True)
    except:
        # Damn; python 2.6; just ignore it
        pass

    # Log to console
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter('%(levelname)s:%(filename)s:%(lineno)d -- %(message)s'))
    logging.getLogger().addHandler(console)

    # Log to syslog
    syslog = SysLogHandler(address='/dev/log')
    syslog.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)s:%(filename)s:%(lineno)d -- %(message)s'))
    logging.getLogger().addHandler(syslog)

    # Run the program
    try:
        main()
    except Exception as ex:
        logging.exception('Unexpected exception! Death is me.')
        exit(1)
