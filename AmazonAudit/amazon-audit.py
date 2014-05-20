#!/usr/bin/python2

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

AWS_HISTORY_FILE_VERSTR = "AWSHistory.1"

_config = None
_civiDB = None
_stompLink = None
_awsLink = None
_logFile = None

def main(secondsToAudit, configFile, gracePeriod, historyFile, logFile, auditPayments, auditRefunds):
    global _config
    global _civiDB
    global _awsLink
    global _stompLink
    global _logFile

    startTime = datetime.fromtimestamp(int(time.time()) - int(secondsToAudit), pytz.utc)
    endTime = datetime.fromtimestamp(int(time.time()) - int(gracePeriod), pytz.utc)
    print("AWS audit requested from %s to %s" % (startTime.isoformat(), endTime.isoformat()))

    # === Initialize the configuration file ===
    localdir = os.path.dirname(os.path.abspath(__file__))
    _config = SafeConfigParser()
    fileList = ["%s/amazon-audit.cfg" % localdir]
    if configFile is not None:
        fileList.append(configFile)
    _config.read(fileList)

    # === Open up ze STOMP ===
    _stompLink = DistStomp(config.get('Stomp', 'server'), config.getint('Stomp', 'port'))
    _stompLink.connect()
    
    # === Connection to Amazon ===
    _awsLink = Amazon(
        awsEndpoint=_config.get('AwsConfig', 'endpoint'),
        awsAccessKey=_config.get('AwsConfig', 'accessKey'),
        awsSecret=_config.get('AwsConfig', 'secretKey')
    )

    # === Connection to MySQL ===
    _civiDB = MySQL.connect(
        _config.get('MySQL', 'host'),
        _config.get('MySQL', 'user'),
        _config.get('MySQL', 'password'),
        _config.get('MySQL', 'schema')
    )

    # === Open up the history and log files ===
    # If the history file exists, it will modify the start time of this script to be the end time of the
    # history file.
    hfile = None
    historyStart = startTime
    if historyFile and os.path.exists(historyFile):
        hfile = open(historyFile, 'r')
        if hfile.readline().strip() == AWS_HISTORY_FILE_VERSTR:
            historyStart = dateutil.parser.parse(hfile.readline().strip())
            historyEnd = dateutil.parser.parse(hfile.readline().strip())
            startTime = historyEnd
            print("History file modified search period, now %s to %s" % (startTime.isoformat(), endTime.isoformat()))
    else:
        print('Not starting with a valid history file.')

    if logFile:
        _logFile = open(logFile, 'a')
        _logFile.write("!!! Starting run for dates %s -> %s\n" % (startTime.isoformat(), endTime.isoformat()))

    # === Sanity checks ===
    if endTime < startTime:
         startTime = endTime
    
    # === Main Application ===
    awsTransactions = []

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
            awsTransactions.append(json.loads(txn))
        hfile.close()

    # --- Obtain AWS history ---
    if auditPayments:
        print("Obtaining AWS payment transactions for the period %s -> %s" % (startTime.isoformat(), endTime.isoformat()))
        awsTransactions += _awsLink.getAccountActivity(startTime, endDate=endTime, fpsOperation='Pay')
        print("Obtained %d transactions" % len(awsTransactions))

    if auditRefunds:
        print("Obtaining AWS refund transactions for the period %s -> %s" % (startTime.isoformat(), endTime.isoformat()))
        awsTransactions += _awsLink.getAccountActivity(startTime, endDate=endTime, fpsOperation='Refund')
        print("Obtained %d transactions" % len(awsTransactions))
    
    # --- Main loop: checks each aws transaction against the Civi database; adding it if it doesn't exist ---
    txncount = 0
    for txn in awsTransactions:
        txncount += 1
        result = dispatchTransaction(txn, auditPayments, auditRefunds)
        historyStats[result] += 1
        if result == 'Pending':
            historyList.append(txn)

    print("\n--- Finished processing of messages. ---\n")

    # --- Write the history file ---
    if historyFile:
        print("Rewriting history file with %d transactions" % len(historyList))
        hfile = open(historyFile, 'w')
        hfile.write("%s\n%s\n%s\n" % (AWS_HISTORY_FILE_VERSTR, historyStart.isoformat(), endTime.isoformat()))
        for txn in historyList:
            hfile.write("%s\n" % json.dumps(txn))
        print("Flushing history file in preparation for main loop")
        hfile.flush()

    # --- Final statistics
    print("Processed %d AWS messages" % txncount)
    print(" ... of which %d messages were from history" % historyCount)
    print("This resulted in the following:")
    for entry in historyStats.items():
        print(" %s Messages: %d" % entry)

    # === Final Application Cleanup ===
    print("\nCleaning up.")
    _civiDB.close()
    _stompLink.disconnect()

    if hfile:
        hfile.close()
    if _logFile:
        _logFile.close()

    time.sleep(1)   # Let the STOMP library catch up


def dispatchTransaction(txn, auditPayments, auditRefunds):
    """Main message processing logic. Will determine if a message needs to be injected or not

    Arguments:
        txn: The transaction from getAccountActivity()
        auditPayments: Boolean, true if 'Pay' statements from AWS are to be audited
        auditRefunds: Boolean, true if 'Refund' statements from AWS are to be audited

    Returns:
        "Success" on injection
        "Pending" on AWS pending
        "Failed" on AWS failure
        "Ignored" on already present in Civi
    """

    global _awsLink
    global _logFile

    smallString = '.'
    ctid = '?'

    if (txn['TransactionStatus'] != 'Failure') and (not isTxnInCivi(txn['TransactionId'])):
        # Get additional information about the transaction because getAccountActivity does not provide all
        # the required information. We also have to check what the status of this transaction is.
        txnInfo = _awsLink.getTransaction( txn['TransactionId'] )

        # Do that aforementioned status check
        if txnInfo['TransactionStatus'] == 'Success':
            if (txn['FPSOperation'] == 'Pay') and auditPayments:
                ctid = injectPaymentMessage(txn, txnInfo)
                retval = 'Success'
                smallString = '+'
            elif (txn['FPSOperation'] == 'Refund') and auditRefunds:
                ctid = injectRefundTransaction(txn, txnInfo)
                retval = 'Success'
                smallString = '-'
            else:
                retval = 'Pending'
                smallString = '!'
        elif txnInfo['TransactionStatus'] == 'Pending' or txnInfo['TransactionStatus'] == 'Reserved':
            retval = 'Pending'
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

    if _logFile:
        _logFile.write("%s\n" % bigString)
        sys.stdout.write(smallString)
        sys.stdout.flush()
    else:
        print(bigString)

    return retval


def isTxnInCivi(txnid):
    """ Query the Civi database to determine if the txnid is present.

    Arguments:
        txnid: The Amazon transaction ID

    Returns:
        True if transaction is present in Civi
    """

    global _civiDB

    cur = _civiDB.cursor()
    cur.execute("SELECT id FROM civicrm_contribution WHERE trxn_id LIKE 'AMAZON %s%%';" % txnid)
    rc = cur.rowcount
    cur.close()
    if rc >= 1:
        return True
    else:
        return False


def extractCtidFromAws(txn):
    """Attempts to extract a contribution tracking ID (CTID) from the return of getAccountActivity

    Arguments:
        txn: The AWS transaction response from

    Returns:
        Integer CTID if found, else None
    """

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

    return ctid


def injectPaymentMessage(txn, txnInfo):
    """Injects a new message into the queue for consumption by Civi

    Arguments:
        txn: The transaction data given from the getAccountActivity call

    Returns:
        An integer contribution tracking ID or None if no CTID can be found
    """

    global _config
    global _stompLink

    # --- Get our contribution tracking ID (but it's stupid because AWS is stupid... ugh!)
    ctid = extractCtidFromAws(txn)

    # Construct the STOMP message
    headers = {
        'correlation-id': 'amazon-%s' % txn['TransactionId'],
        'destination': _config.get('Stomp', 'verified-queue'),
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
        "original_currency": txnInfo['TransactionAmount']['CurrencyCode'],
        "original_gross": txnInfo['TransactionAmount']['Value'],
        "fee":"0",
        "gross": txnInfo['TransactionAmount']['Value'],
        "net": txnInfo['TransactionAmount']['Value'],

        "date": dateutil.parser.parse(txnInfo['DateReceived']).astimezone(dateutil.tz.tzutc()).strftime('%s'),

        "gateway":"amazon",
        "gateway_account": _config.get('AwsConfig', 'accountName'),
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

    frame = headers
    frame['body'] = json.dumps(msg)

    # Inject the message
    _stompLink.send(frame)

    return ctid


def injectRefundTransaction(txn, txnInfo):
    """Injects a new message into the refund queue for consumption by Civi

    Arguments:
        txn: The transaction data given from the getAccountActivity call
        txnInfo: Transaction data given from the getTransaction call

    Returns:
        The contribution tracking ID
    """

    global _config
    global _stompLink

    # --- It appears AWS does not retain the merchant reference for refunds; it does however
    # give us a FPS parent transaction ID
    ctid = 'Refund'
    orig_txnid = txn['OriginalTransactionId']

    # Construct the STOMP message
    headers = {
        'correlation-id': 'amazon-%s' % orig_txnid,
        'destination': _config.get('Stomp', 'refund-queue'),
        'persistent': 'true'
    }
    msg = {
        "gateway_refund_id": txn['TransactionId'],
        "gateway_parent_id": orig_txnid,

        "gross_currency": txnInfo['TransactionAmount']['CurrencyCode'],
        "gross": txnInfo['TransactionAmount']['Value'],

        "fee_currency": txn['FPSFees']['CurrencyCode'],
        "fee": abs(float(txn['FPSFees']['Value'])),

        "type": 'refund',

        "date": dateutil.parser.parse(txnInfo['DateCompleted']).astimezone(dateutil.tz.tzutc()).strftime('%s'),

        "gateway":"amazon",
        "gateway_account": _config.get('AwsConfig', 'accountName'),
        "payment_method":"amazon",
        "payment_submethod": txn['PaymentMethod'],
    }

    # Inject the message
    _stompLink.send(
        json.dumps(msg),
        headers
    )

    return ctid


if __name__ == "__main__":
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] <# of seconds to audit>")
    parser.add_option("-c", "--config", dest='configFile', default=None, help='Path to configuration file')
    parser.add_option("-g", "--gracePeriod", dest='gracePeriod', default=0, help='Number of seconds from now backwards to ignore')
    parser.add_option("-i", "--historyFile", dest='historyFile', default=None, help='Stores any pending transactions and the last run time')
    parser.add_option('-l', "--logFile", dest='logFile', default=None, help='Saves a log of all Amazon transactions')
    parser.add_option('--auditPayments', dest='auditPayments', action='store_true', default=False, help='Audit payment operations.')
    parser.add_option('--auditRefunds', dest='auditRefunds', action='store_true', default=False, help='Audit refund operations.')
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_usage()
        exit()

    # === Launch the application ===
    main(
        secondsToAudit=args[0],
        configFile=options.configFile,
        gracePeriod=options.gracePeriod,
        historyFile=options.historyFile,
        logFile=options.logFile,
        auditPayments=options.auditPayments,
        auditRefunds=options.auditRefunds
    )
