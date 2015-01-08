#!/usr/bin/python

from amazon import Amazon
# FIXME: reuse stomp_wrap
from stompy import Stomp as DistStomp
from ConfigParser import SafeConfigParser
from optparse import OptionParser
from datetime import datetime
import dateutil.parser
import time
import pytz
import MySQLdb as MySQL
import json
import sys
import os

AWS_HISTORY_FILE_VERSTR = "AWSChargebackHistory.1"

def main():
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] <# of seconds to audit>")
    parser.add_option("-c", "--config", dest='configFile', default=None, help='Path to configuration file')
    parser.add_option("-g", "--gracePeriod", dest='gracePeriod', default=0, help='Number of seconds from now backwards to ignore')
    parser.add_option("-i", "--historyFile", dest='historyFile', default=None, help='Stores any pending transactions and the last run time')
    parser.add_option('-l', "--logFile", dest='logFile', default=None, help='Saves a log of all Amazon transactions')
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_usage()
        exit()

    startTime = datetime.fromtimestamp(int(time.time()) - int(args[0]), pytz.utc)
    endTime = datetime.fromtimestamp(int(time.time()) - int(options.gracePeriod), pytz.utc)
    print("AWS refund audit requested from %s to %s" % (startTime.isoformat(), endTime.isoformat()))

    # === Get the configuration options ===
    config = SafeConfigParser()
    fileList = ['./amazon-config.cfg']
    if options.configFile is not None:
        fileList.append(options.configFile)
    config.read(fileList)

    # === Open up ze STOMP ===
    sc = DistStomp(config.get('Stomp', 'server'), config.getint('Stomp', 'port'))
    sc.connect()

    # === Connection to Amazon ===
    aws = Amazon(
        awsEndpoint = config.get('AwsConfig', 'endpoint'),
        awsAccessKey = config.get('AwsConfig', 'accessKey'),
        awsSecret = config.get('AwsConfig', 'secretKey')
    )

    # === Connection to MySQL ===
    dbcon = MySQL.connect(
        config.get('MySQL', 'host'),
        config.get('MySQL', 'user'),
        config.get('MySQL', 'password'),
        config.get('MySQL', 'schema')
    )

    # === Open up the history and log files ===
    # If the history file exists, it will modify the start time of this script to be the end time of the
    # history file.
    hfile = None
    historyStart = startTime
    historyEnd = endTime
    if options.historyFile and os.path.exists(options.historyFile):
        hfile = open(options.historyFile, 'r')
        if hfile.readline().strip() == AWS_HISTORY_FILE_VERSTR:
            historyStart = dateutil.parser.parse(hfile.readline().strip())
            historyEnd = dateutil.parser.parse(hfile.readline().strip())
            startTime = historyEnd
            print("History file modified search period, now %s to %s" % (startTime.isoformat(), endTime.isoformat()))
    else:
        print('Not starting with a valid history file.')

    sfile = None
    if options.logFile:
        sfile = open(options.logFile, 'a')
        sfile.write("!!! Starting run for dates %s -> %s\n" % (startTime.isoformat(), endTime.isoformat()))

    # === Sanity checks ===
    if endTime < startTime:
         startTime = endTime

    # === Main Application ===
    # --- Process all previously pending transactions from the history file. If the transaction is still in some form
    #     of pending, add it back to the history list.
    historyCount = 0
    historyList = []
    historyStats = {
        'Success': 0,
        'Pending': 0,
        'Failed': 0,
        'Ignored': 0
    }
    if hfile:
        print("Processing history file")
        for txn in hfile:
            historyCount += 1
            txn = json.loads(txn)
            result = processTransaction(txn, dbcon, aws, sc, sfile, config)
            historyStats[result] += 1
            if result == 'Pending':
                historyList.append(txn)
        hfile.close()

    # --- Obtain AWS history ---
    print("Obtaining AWS transactions for the period %s -> %s" % (startTime.isoformat(), endTime.isoformat()))
    awsTransactions = aws.getAccountActivity(startTime, endDate=endTime, fpsOperation='Pay')
    print("Obtained %d transactions" % len(awsTransactions))

    # --- Main loop: checks each aws transaction against the Civi database; adding it if it doesn't exist ---
    txncount = 0
    for txn in awsTransactions:
        txncount += 1
        result = processTransaction(txn, dbcon, aws, sc, sfile, config)
        historyStats[result] += 1
        if result == 'Pending':
            historyList.append(txn)

    print("\n--- Finished processing of messages. ---\n")

    # --- Prepare the history file for write ---
    if options.historyFile:
        print("Rewriting history file with %d transactions" % len(historyList))
        hfile = open(options.historyFile, 'w')
        hfile.write("%s\n%s\n%s\n" % (AWS_HISTORY_FILE_VERSTR, historyStart.isoformat(), endTime.isoformat()))
        for txn in historyList:
            hfile.write("%s\n" % json.dumps(txn))
        print("Flushing history file in preparation for main loop")
        hfile.flush()

    # --- Final statistics ---
    print("%d new AWS messages" % txncount)
    print(" Additionally %d messages were processed from history" % historyCount)
    print("This resulted in the following:")
    for entry in historyStats.items():
        print(" %s Messages: %d" % entry)

    # === Final Application Cleanup ===
    print("\nCleaning up.")
    dbcon.close()
    sc.disconnect()

    if hfile:
        hfile.close()
    if sfile:
        sfile.close()

    time.sleep(1)   # Let the STOMP library catch up

def processTransaction(txn, dbcon, aws, sc, sfile, config):
    """Main message processing logic. Will determine if a message needs to be injected or not

    txn -- The transaction from getAccountActivity()
    aws -- The AWS connection object
    sc -- The Stomp connection object
    sfile -- The log file
    config -- The configuration object

    returns "Success on injection, Pending on AWS pending, Failed on AWS failure, Ignored on already present in Civi
    """
    retval = ''
    smallString = '.'
    ctid = '?'

    if (txn['TransactionStatus'] != 'Failure') and (not isTxnInCivi(txn['TransactionId'], dbcon)):
        # Get additional information about the transaction because getAccountActivity does not provide all
        # the required information. We also have to check what the status of this transaction is.
        txnInfo = aws.getTransaction( txn['TransactionId'] )

        # Do that aforementioned status check
        if txnInfo['TransactionStatus'] == 'Success':
            ctid = remediateTransaction(txn, txnInfo, sc, config)
            retval = 'Success'
            smallString = '+'
        elif txnInfo['TransactionStatus'] == 'Pending' or txnInfo['TransactionStatus'] == 'Reserved':
            retval = 'Pending'
            smallString = '-'
        else:
            retval = 'Failed'
    else:
        retval = 'Ignored'

    bigString = "%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
        retval,
        txn['TransactionStatus'],
        txn['TransactionId'],
        ctid if ctid is not None else '?',
        dateutil.parser.parse(txn['DateReceived']).astimezone(dateutil.tz.tzutc()).isoformat(),
        dateutil.parser.parse(txn['DateCompleted']).astimezone(dateutil.tz.tzutc()).isoformat(),
        datetime.now(dateutil.tz.tzutc()).isoformat()
    )

    if sfile:
        sfile.write("%s\n" % bigString)
        sys.stdout.write(smallString)
        sys.stdout.flush()
    else:
        print(bigString)

    return retval

def isTxnInCivi(txnid, dbcon):
    """ Query the Civi database to determine if the txnid is present.

    txnid -- The Amazon transaction ID
    dbcon -- Database connection object

    returns True if transaction is present in Civi
    """

    cur = dbcon.cursor()
    cur.execute("SELECT trxn_id FROM civicrm_contribution WHERE trxn_id LIKE 'AMAZON %s%%';" % txnid)
    rc = cur.rowcount
    cur.close()
    if rc >= 1:
        return True
    else:
        return False

def remediateTransaction(txn, txnInfo, sc, config):
    """Injects a new message into the queue for consumption by Civi

    txn -- The transaction data given from the getAccountActivity call
    txnInfo -- Transaction data given from the getTransaction call
    sc -- Stomp queue object
    config -- Configuration object

    returns -- The contribution tracking ID
    """

    # --- Get our contribution tracking ID (but it's stupid because AWS is stupid... ugh!)
    ctid = None
    if isinstance(txn['TransactionPart'], list):
        for part in txn['TransactionPart']:
            if part['Role'] == 'Recipient' and 'Reference' in part:
                ctid = part['Reference']
    elif txn['TransactionPart']['Role'] == 'Recipient' and 'Reference' in txn['TransactionPart']:
        ctid = txn['TransactionPart']['Reference']

    if ctid is not None:
        try:
            ctid = int(ctid)
        except ValueError:
            if not '-' in ctid:
                # It's not a number or a UUID... very strange... not using is
                ctid = None

    # Construct the STOMP message
    headers = {
        'correlation-id': 'amazon-%s' % txn['TransactionId'],
        'destination': config.get('Stomp', 'verified-queue'),
        'persistent': 'true'
    }
    msg = {
        "contribution_tracking_id": ctid if ctid is not None else '',
        "gateway_txn_id": txn['TransactionId'],

        "email": txnInfo['SenderEmail'],
        "first_name": txnInfo['SenderName'].split(' ')[0],
        "middle_name":"",
        "last_name": " ".join(txnInfo['SenderName'].split(' ')[1:]),
        "last_name_2":"",

        "currency": txnInfo['TransactionAmount']['CurrencyCode'],
        "fee":"0",
        "gross": txnInfo['TransactionAmount']['Value'],
        "net": txnInfo['TransactionAmount']['Value'],

        "date": dateutil.parser.parse(txnInfo['DateReceived']).astimezone(dateutil.tz.tzutc()).strftime('%s'),

        "gateway":"amazon",
        "gateway_account": config.get('AwsConfig', 'accountName'),
        "payment_method":"amazon",
        "payment_submethod": txn['PaymentMethod'],
        "referrer":"",
        "comment":"", "size":"",
        "premium_language":"en",
        "language":"en",
        "utm_source":"..amazon",
        "utm_medium":"",
        "utm_campaign":"",
        "street_address":"",
        "supplemental_address_1":"",
        "city":"",
        "state_province":"",
        "country":"",
        "street_address_2":"",
        "supplemental_address_2":"",
        "city_2":"",
        "state_province_2":"",
        "country_2":"",
        "postal_code_2":"",
        "user_ip":"",
        "response":False,
        "recurring":""
    }

    frame = {}
    frame.update(headers)
    frame['body'] = json.dumps(msg)

    # Inject the message
    sc.send(frame)

    return ctid

if __name__ == "__main__":
    main()
