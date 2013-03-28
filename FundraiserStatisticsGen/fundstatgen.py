#!/usr/bin/python

import sys
import MySQLdb as db
import csv
from optparse import OptionParser
from ConfigParser import SafeConfigParser
from operator import itemgetter

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
    fileList = ['./fundstatgen.cfg']
    if options.configFile is not None:
        fileList.append(options.configFile)
    config.read(fileList)

    # === BEGIN PROCESSING ===
    print("Running query...")
    stats = getPerYearData(
        config.get('MySQL', 'hostname'),
        config.getint('MySQL', 'port'),
        config.get('MySQL', 'username'),
        config.get('MySQL', 'password'),
        config.get('MySQL', 'database')
    )

    print("Pivoting data into year/day form...")
    (years, pivot) = pivotDataByYear(stats)

    print("Writing output files...")
    createSingleOutFile(stats, 'date', workingDir + '/donationdata-vs-day.csv')
    createOutputFiles(pivot, 'date', workingDir + '/yeardata-day-vs-', years)


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
          DATE_FORMAT(receive_date, "%Y-%m-%d") as receive_date,
          SUM(IF(total_amount >= 0, total_amount, 0)) as credit,
          SUM(IF(total_amount >= 0, 1, 0)) as credit_count,
          SUM(IF(total_amount < 0, total_amount, 0)) as refund,
          SUM(IF(total_amount < 0, 1, 0)) as refund_count,
          AVG(IF(total_amount >= 0, total_amount, 0)) as `avg`,
          MAX(total_amount)
        FROM civicrm_contribution
        WHERE receive_date >= '2006-01-01'
        GROUP BY DATE_FORMAT(receive_date, "%Y-%m-%d") ASC;
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
            if ('2006/%s/%s 23:59:59' % (month, day)) not in pivot[report]:
                pivot[report]['2006/%s/%s 23:59:59' % (month, day)] = {}
            pivot[report]['2006/%s/%s 23:59:59' % (month, day)][year] = stats[date][report]

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
        createSingleOutFile(stats[report], firstcol, basename + report + '.csv', colnames)


def createSingleOutFile(stats, firstcol, filename, colnames = None):
    """
    Creates a single report file from a keyed dict
    """
    if colnames is None:
        colnames = stats.itervalues().next().keys()
        colindices = colnames
    else:
        colindices = range(0, len(colnames))

    f = file(filename, 'w')
    csvf = csv.writer(f)
    csvf.writerow([firstcol] + colnames)

    for linekey in sorted(stats.keys()):
        csvf.writerow([linekey] + [stats[linekey][col] for col in colindices])
    f.close()


if __name__ == "__main__":
    main()
