#!/usr/bin/python

from amazon import Amazon
import stomp
from ConfigParser import SafeConfigParser
from optparse import OptionParser
from datetime import datetime
import dateutil.parser
import time
import MySQLdb as MySQL
import json
import sys

def main():
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] <# of seconds to audit>")
    parser.add_option("-c", "--config", dest='configFile', default=None, help='Path to configuration file')
    parser.add_option("-g", "--gracePeriod", dest='gracePeriod', default=0, help='Number of seconds from now backwards to ignore')
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_usage()
        exit()
        
    startTime = datetime.fromtimestamp(int(time.time()) - int(args[0]))
    endTime = datetime.fromtimestamp(int(time.time()) - options.gracePeriod)
    print("Starting AWS audit from %s to %s" % (startTime.isoformat(), endTime.isoformat()))

    # === Get the configuration options ===
    config = SafeConfigParser()
    fileList = ['./amazon-config.cfg']
    if options.configFile is not None:
        fileList.append(options.configFile)
    config.read(fileList)

    # === Open up ze STOMP ===
    host_and_ports = (config.get('Stomp', 'server'), config.getint('Stomp', 'port'))
    sc = stomp.Connection(host_and_ports=[host_and_ports])
    sc.start()
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
    
    # === Main Application ===
    # --- Obtain AWS information ---
    print("Obtaining AWS transactions for the period")
    awsTransactions = aws.getAccountActivity( startTime, endDate=endTime, fpsOperation='Pay', status='Success' )
    print("Obtained %d transactions" % len(awsTransactions))
    
    # --- Main loop: checks each aws transaction against the Civi database; adding it if it doesn't exist ---
    injectCount = 0
    for txn in awsTransactions:
        # --- Query Civi DB
        cur = dbcon.cursor()
        cur.execute("SELECT trxn_id FROM civicrm_contribution WHERE trxn_id LIKE 'AMAZON %s%%';" % txn['TransactionId'])
        rc = cur.rowcount
        cur.close()
        if rc >= 1:
            print("Amazon txnid %s was found more than once in Civi! Ignoring this txnid." % txn['TransactionId'])
            continue

        # --- If we're here there's no entry in Civi. However the getAccountActivity call does not provide all the info
        #       we need, so make another one, txn specific.
        txnInfo = aws.getTransaction( txn['TransactionId'] )

        # --- Confirm that the transaction has in fact completed before constructing the STOMP message
        if txnInfo['StatusCode'] != 'Success':
            sys.stderr.write("Amazon changed the status of %s on us!" % txn['TransactionId'])
            continue

        # Construct the STOMP message
        headers = {
            'correlation-id': 'amazon-%s' % txnInfo['CallerReference'],
            'destination': config.get('Stomp', 'verified-queue'),
            'persistent': 'true'
        }
        msg = {
            "contribution_tracking_id": '',
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

        # Inject the message
        print("Injecting found message into queue. Transaction ID: %s, Contribution ID: %s" % (txn['TransactionId'], txnInfo['CallerReference']))
        sc.send(
            json.dumps(msg),
            headers
        )
        injectCount += 1

    print("Of %d messages, %d were not found and were injected." % (len(awsTransactions), injectCount))

    # === Final Application Cleanup ===
    print("Finished. Cleaning up.")
    dbcon.close()
    sc.disconnect()
    time.sleep(1)   # Let the STOMP library catch up

if __name__ == "__main__":
    main()
