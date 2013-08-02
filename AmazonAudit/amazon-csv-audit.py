#!/usr/bin/python
# Using a CSV file downloaded from the Amazon Payments portal, check that all expected transactions
# are present in Civi.

import csv
import sys
from dateutil.parser import parse as dparse
import MySQLdb as MySQL

if len(sys.argv) != 5:
    print("Expected path to CSV file, dbhost, dbuser, dbpass")
    exit(-1)

r = csv.reader(file(sys.argv[1],'r'))

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
    if rc == 1:
        return True
    elif rc > 1:
        print "%s duplicated!" % txnid
        return True
    else:
        return False

dbcon = MySQL.connect(
    sys.argv[2],
    sys.argv[3],
    sys.argv[4],
    'civicrm'
)

# Header line
r.next()

found = 0
notfound = 0
pending = 0
other = 0

for record in r:
    date = record[0]
    type = record[1]
    tf = record[2]
    donorName = record[3]
    status = record[4]
    amount = record[5]
    fees = record[6]
    txnid = record[7]

    if type=='Payment' and tf=='From':
        if status=="Completed":
            if not isTxnInCivi(txnid, dbcon):
                print "%s - %s not found" % (date, txnid)
                notfound += 1
            else:
                found += 1
        elif status=="Pending":
            print "%s pending!" % txnid
            pending += 1
        else:
            other+=1

dbcon.close()

print("Found: %d\nNot Found: %d\nPending: %d\nOther: %d" % (found, notfound, pending, other))
