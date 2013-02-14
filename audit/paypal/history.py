#!/usr/bin/env python

from ConfigParser import SafeConfigParser
from optparse import OptionParser
import stomp
import time
import json
import csv
import atexit

def main():
    global config, messaging, options
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-c", "--config", dest='configFile', default=[ "paypal-audit.cfg" ], action='append', help='Path to configuration file')
    parser.add_option("-f", "--auditFile", dest='auditFile', default=None, help='CSV of transaction history')
    parser.add_option('-l', "--logFile", dest='logFile', default="audit.log", help='Destination logfile. New messages will be appended.')
    (options, args) = parser.parse_args()

    path = options.auditFile
    infile = csv.DictReader(open(path, "rU"))

    config = SafeConfigParser()
    config.read(options.configFile)

    messaging = Stomp(config)

    # fix spurious whitespace around column header names
    infile.fieldnames = [ name.strip() for name in infile.fieldnames ]

    ipn_handled_types = [
        "Subscription Payment Received",
        "Web Accept Payment Received",
        "Shopping Cart Payment Received",
        "Virtual Debt Card Credit Received",
        "Payment Received",
        "Update to eCheck Received",
    ]

    ignore_types = [
        "Cancelled Fee",
        # currency conversion is an explanation of amounts which appear elsewhere
        "Currency Conversion",
        # TODO: handle in IPN
        "Temporary Hold",
        # seems to be the cancellation of a temporary hold
        "Update to Reversal",
    ]

    audit_dispatch = {
        "Reversal": handle_refund,
        "Chargeback Settlement": handle_refund,
        "Refund": handle_refund,
    }

    for line in infile:
        if line['Type'] in ipn_handled_types + ignore_types:
            log("Ignoring %s of type %s" % (line['Transaction ID'], line['Type']))
            continue
        if line['Type'] in audit_dispatch:
            audit_dispatch[line['Type']](line)
        else:
            handle_unknown(line)

def handle_unknown(line):
    log("Unhandled transaction, type \"%s\": %s" % (line['Type'], json.dumps(line)))

def handle_refund(line):
    global config, messaging

    if line['Status'] != "Completed":
        return handle_unknown(line)

    txn_id = line['Transaction ID']

    # Construct the STOMP message
    headers = {
        'correlation-id': 'paypal-%s' % txn_id,
        'destination': config.get('Stomp', 'refund-queue'),
        'persistent': 'true',
    }
    msg = normalize_msg(line)

    log("Queueing refund %s" % (txn_id, ))
    messaging.send(headers, msg)

def normalize_msg(line):
    refund_type = "unknown"
    if line['Type'] == "Refund":
        refund_type = "refund"
    elif line['Type'] == "Chargeback Settlement":
        refund_type = "chargeback"
    elif line['Type'] == "Reversal":
        refund_type = "reversal"

    return {
        'date': line['Date'] + " " + line['Time'] + " " + line['Time Zone'],
        'email': line['From Email Address'],
        'gross_currency': line['Currency'],
        'gross': line['Gross'],
        'fee': line['Fee'],
        'net': line['Net'],
        'gateway_refund_id': line['Transaction ID'],
        'gateway_parent_id': line['Reference Txn ID'],
        'gateway': "paypal",
        'type': refund_type,
    }


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
            time.sleep(1)

    def send(self, headers=None, msg=None):
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
