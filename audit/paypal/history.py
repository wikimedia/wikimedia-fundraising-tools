#!/usr/bin/env python

from ConfigParser import SafeConfigParser
from optparse import OptionParser
import stomp
import time
import json
import csv
import atexit
import re
import gzip
import locale
import dateutil.parser
from civicrm.civicrm import Civicrm

def main():
    global config, messaging, options, civi
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-c", "--config", dest='configFile', default=[ "paypal-audit.cfg" ], action='append', help='Path to configuration file')
    parser.add_option("-f", "--auditFile", dest='auditFile', default=None, help='CSV of transaction history')
    parser.add_option('-l', "--logFile", dest='logFile', default="audit.log", help='Destination logfile. New messages will be appended.')
    parser.add_option("-n", "--no-effect", dest='noEffect', default=False, action="store_true", help="Dummy no-effect mode")
    (options, args) = parser.parse_args()

    path = options.auditFile
    if re.search(r'[.]gz$', path):
        f = gzip.open(path, "rb")
    else:
        f = open(path, "rU")
    infile = csv.DictReader(f)

    config = SafeConfigParser()
    config.read(options.configFile)

    if options.noEffect:
        log("*** Dummy mode! Not injecting stomp messages ***")

    messaging = Stomp(config)
    civi = Civicrm(config)

    locale.setlocale(locale.LC_NUMERIC, "")

    # fix spurious whitespace around column header names
    infile.fieldnames = [ name.strip() for name in infile.fieldnames ]

    ignore_types = [
        "Authorization",
        "Cancelled Fee",
        # currency conversion is an explanation of amounts which appear elsewhere
        "Currency Conversion",
        # TODO: handle in IPN
        "Temporary Hold",
        # seems to be the cancellation of a temporary hold
        "Update to Reversal",
        "Website Payments Pro API Solution",
    ]

    audit_dispatch = {
        "Reversal": handle_refund,
        "Chargeback Settlement": handle_refund,
        "Refund": handle_refund,

        "Subscription Payment Received": handle_payment,
        "Web Accept Payment Received": handle_payment,
        "Shopping Cart Payment Received": handle_payment,
        "Virtual Debt Card Credit Received": handle_payment,
        "Payment Received": handle_payment,
        "Update to eCheck Received": handle_payment,
    }

    for line in infile:
        if line['Type'] in ignore_types:
            log("Ignoring %s of type %s" % (line['Transaction ID'], line['Type']))
            continue
        if line['Type'] in audit_dispatch:
            audit_dispatch[line['Type']](line)
        else:
            handle_unknown(line)

def handle_unknown(line):
    log("Unhandled transaction, type \"%s\": %s" % (line['Type'], json.dumps(line)))

def handle_refund(line):
    global config, messaging, civi

    if line['Status'] != "Completed":
        return handle_unknown(line)

    txn_id = line['Transaction ID']

    # Construct the STOMP message
    msg = normalize_refund_msg(line)

    if not civi.transaction_exists(line['Reference Txn ID']):
        log("Refund missing parent: %s" % (json.dumps(msg), ))
    elif not civi.transaction_exists(txn_id):
        log("Queueing refund %s" % (txn_id, ))
        messaging.send(msg, "refund")
    else:
        log("Refund already exists: %s" % (txn_id, ))

def handle_payment(line):
    global config, messaging, civi

    if line['Status'] != "Completed":
        return handle_unknown(line)

    txn_id = line['Transaction ID']

    # Construct the STOMP message
    msg = normalize_msg(line)

    if not civi.transaction_exists(txn_id):
        log("Queueing payment %s" % (txn_id, ))
        messaging.send(msg, "payment")
    else:
        log("Payment already exists: %s" % (txn_id, ))

def normalize_msg(line):
    timestamp = dateutil.parser.parse(
        line['Date'] + " " + line['Time'] + " " + line['Time Zone'],
    ).strftime("%s")

    names = line['Name'].split(" ")

    return {
        'date': timestamp,
        'email': line['From Email Address'],

        'first_name': names[0],
        'last_name': " ".join(names[1:]),

        'street_address': line['Address Line 1'],
        'supplemental_address_1': line['Address Line 2/District/Neighborhood'],
        'city': line['Town/City'],
        'state_province': line['State/Province/Region/County/Territory/Prefecture/Republic'],
        'country': line['Country'],
        'postal_code': line['Zip/Postal Code'],

        'comment': line['Note'],
        # somthing with: line['Subscription Number'],

        'original_currency': line['Currency'],

        'gross_currency': line['Currency'],
        'gross': round(locale.atof(line['Gross']), 2),
        'fee': round(locale.atof(line['Fee']), 2),
        'net': round(locale.atof(line['Net']), 2),
        'gateway': "paypal",
        'gateway_txn_id': line['Transaction ID'],
    }

def normalize_refund_msg(line):
    msg = normalize_msg(line)

    refund_type = "unknown"
    if line['Type'] == "Refund":
        refund_type = "refund"
    elif line['Type'] == "Chargeback Settlement":
        refund_type = "chargeback"
    elif line['Type'] == "Reversal":
        refund_type = "reversal"

    msg.update({
        'gross': 0 - msg['gross'],
        'fee': 0 - msg['fee'],
        'net': 0 - msg['net'],
        'type': refund_type,
        'gateway_refund_id': line['Transaction ID'],
        'gateway_parent_id': line['Reference Txn ID'],
    })

    return msg

class Stomp(object):
    def __init__(self, config):
        host_and_ports = [(config.get('Stomp', 'server'), config.getint('Stomp', 'port'))]
        self.sc = stomp.Connection(host_and_ports)
        self.sc.start()
        self.sc.connect()

    def __del__(self):
        if self.sc:
            self.sc.disconnect()

            # Let the STOMP library catch up
            import time
            time.sleep(1)

    def send(self, msg, queue_name):
        global options, config

        if options.noEffect:
            log("not queueing message. " + json.dumps(msg))
            return

        headers = {
            'correlation-id': '%s-%s' % (msg['gateway'], msg['gateway_txn_id']),
            'destination': config.get('Stomp', '%s-queue' % (queue_name,)),
            'persistent': 'true',
        }

        if config.getboolean('Stomp', 'debug'):
            log("sending %s %s" % (headers, msg))

        self.sc.send(
            json.dumps(msg),
            headers
        )

log_file = None

def log(msg):
    global options, log_file
    if not log_file:
        log_file = open(options.logFile, 'a')
        atexit.register(file.close, log_file)
    log_file.write(msg + "\n")


if __name__ == "__main__":
    main()
