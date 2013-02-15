#!/usr/bin/env python
'''
Parse GlobalCollect history dump and compare with Civi.

Results are kept in a scratch table.
'''

from ConfigParser import SafeConfigParser
from optparse import OptionParser
import json
import csv
import atexit

from db import Connection as DbConnection

def main():
    global config, options, db
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-c", "--config", dest='configFile', default=[ "globalcollect-audit.cfg" ], action='append', help='Path to configuration file')
    parser.add_option("-f", "--auditFile", dest='auditFile', default=None, help='CSV of transaction history')
    parser.add_option('-l', "--logFile", dest='logFile', default="audit.log", help='Destination logfile. New messages will be appended.')
    (options, args) = parser.parse_args()

    path = options.auditFile
    infile = csv.DictReader(open(path, "rU"), delimiter=";")

    config = SafeConfigParser()
    config.read(options.configFile)

    db = DbConnection(**config._sections['mysql'])

    for line in infile:
        # TODO parse and filter on status ids
        #if line["Status Description"] is not "COLLECTED":

        normalized = {
            'transaction_id': line["Order ID"],
            'currency': line["Order Currency Code"],
            'amount': line["Order Amount"],
            'received': line["Received Date"],
            # GC has multiple time formats...
            'time_format': "%Y-%m-%d %H:%i",
            #'time_format': "%c/%e/%y %k:%i",
        }

        sql = """
INSERT IGNORE INTO test.scratch_transactions SET
    transaction_id = %(transaction_id)s,
    currency = %(currency)s,
    amount = %(amount)s,
    received = STR_TO_DATE( %(received)s, %(time_format)s ),
    in_gateway = 1
        """
        db.execute(sql, normalized)

    db.close()


log_file = None

def log(msg):
    global options, log_file
    if not log_file:
        log_file = open(options.logFile, 'a')
        atexit.register(file.close, log_file)
    log_file.write(msg + "\n")


if __name__ == "__main__":
    main()
