#!/usr/bin/python

import MySQLdb as db
import csv
from optparse import OptionParser
from ConfigParser import SafeConfigParser

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

    stats = getData(hostname, port, username, password, database)

    createSingleOutFile(stats, ['date', 'utm_medium'], workingDir + '/donationdata-medium-breakdown.csv')


def getData(host, port, username, password, database):
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
          DATE_FORMAT(c.receive_date, "%Y-%m-%dT%00:00:00+0") as receive_date,
          ct.utm_medium,
          SUM(IF(c.total_amount >= 0, 1, 0)) as credit_count,
          SUM(c.total_amount) as usd_credit_total,
          AVG(c.total_amount) as usd_credit_avg,
          MAX(c.total_amount) as usd_credit_max
        FROM civicrm_contribution c, drupal.contribution_tracking ct
        WHERE
          c.receive_date >= '2012-07-01' AND c.receive_date < '2013-07-01' AND
          c.id = ct.contribution_id
        GROUP BY
          DATE_FORMAT(c.receive_date, "%Y-%m-%dT%00:00:00+0"), ct.utm_medium;
        """)

    data = {}
    for row in cur:
        (date, medium, credit_count, usd_credit_total, usd_credit_avg, usd_credit_max) = row

        data[(date, medium)] = {
            'count': credit_count,
            'usd_total': usd_credit_total,
            'usd_avg': usd_credit_avg,
            'usd_max': usd_credit_max
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
