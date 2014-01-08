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
    hostname = config.get('MySQL', 'hostname')
    port = config.getint('MySQL', 'port')
    username = config.get('MySQL', 'username')
    password = config.get('MySQL', 'password')
    database = config.get('MySQL', 'schema')

    avgs = getAvgs(hostname, port, username, password, database)
    stats = getData(hostname, port, username, password, database, avgs)

    createSingleOutFile(stats, ['date', 'country'], workingDir + '/donationdata-country-breakdown.csv')


def getAvgs(host, port, username, password, database):
    con = db.connect(host=host, port=port, user=username, passwd=password, db=database)
    cur = con.cursor()
    cur.execute("""
        SELECT
          cy.iso_code,
          AVG(IF(c.total_amount>=0, c.total_amount, 0))
        FROM civicrm_contribution c
        INNER JOIN drupal.contribution_tracking ct ON c.id=ct.contribution_id
        LEFT JOIN civicrm_address a ON c.contact_id=a.contact_id
        LEFT JOIN civicrm_country cy ON a.country_id=cy.id
        WHERE
          c.receive_date >= '2012-12-01'
        GROUP BY cy.iso_code;
        """)

    data = {}
    for row in cur:
        (iso, avg) = row
        data[iso] = float(avg)
    return data


def getData(host, port, username, password, database, avgs):
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
          DATE_FORMAT(c.receive_date, "%Y-%m-%dT%H:00:00+0") as receive_date,
          cy.iso_code,
          SUM(IF(c.total_amount >= 0, c.total_amount, 0)) as credit,
          SUM(IF(c.total_amount >= 0, 1, 0)) as credit_count,
          AVG(IF(c.total_amount >= 0, c.total_amount, 0)) as `avg`,
          MAX(c.total_amount) as `max`
        FROM civicrm_contribution c
        INNER JOIN drupal.contribution_tracking ct ON c.id=ct.contribution_id
        LEFT JOIN civicrm_address a ON c.contact_id=a.contact_id
        LEFT JOIN civicrm_country cy ON a.country_id=cy.id
        WHERE
          c.receive_date >= '2012-12-01'
        GROUP BY
          DATE_FORMAT(receive_date, "%Y-%m-%dT%H:00:00+0") ASC,
          cy.iso_code;
        """)

    data = {}
    ytdCreditSum = 0
    cyear = 0
    for row in cur:
        (date, country, credit_sum, credit_count, avg, max) = row
        year = int(date[0:4])
        credit_sum = float(credit_sum)
        credit_count = int(credit_count)
        avg = float(avg)
        max = float(max)

        if (credit_count <= 5):
            # Normalize for donor data protection
            avg = avgs[country]
            credit_sum = credit_count * avg
            max = avg

        if cyear != year:
            ytdCreditSum = 0
        ytdCreditSum += credit_sum

        data[(date, country)] = {
            'sum': credit_sum,
            'donations': credit_count,
            'avg': avg,
            'max': max,
            'ytdsum': ytdCreditSum
        }

    del cur
    con.close()
    return data


def createSingleOutFile(stats, firstcols, filename, colnames = None):
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

    f = file(filename, 'w')
    csvf = csv.writer(f)
    csvf.writerow(firstcols + colnames)

    for linekey in sorted(stats.keys()):
        if isinstance(linekey, basestring):
            linekeyl = [linekey]
        else:
            linekeyl = list(linekey)

        rowdata = [stats[linekey][col] for col in colindices]
        csvf.writerow(linekeyl + rowdata)
    f.close()


if __name__ == "__main__":
    main()
