'''Parser for Transaction Detail Report files

See https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_Gen_TransactionDetailReport.pdf
'''

import logging
import re

import process.globals
import queue.redis_wrap
import ppreport

import civicrm.civicrm
import paypal_api

log = logging.getLogger(__name__)


class TrrFile(object):
    VERSION = [4, 8]
    redis = None
    # FIXME: these are version 8 headers, we would fail on multi-part v4 files...
    column_headers = [
        "Column Type",
        "Transaction ID",
        "Invoice ID",
        "PayPal Reference ID",
        "PayPal Reference ID Type",
        "Transaction Event Code",
        "Transaction Initiation Date",
        "Transaction Completion Date",
        "Transaction Debit or Credit",
        "Gross Transaction Amount",
        "Gross Transaction Currency",
        "Fee Debit or Credit",
        "Fee Amount",
        "Fee Currency",
        "Transactional Status",
        "Insurance Amount",
        "Sales Tax Amount",
        "Shipping Amount",
        "Transaction Subject",
        "Transaction Note",
        "Payer's Account ID",
        "Payer Address Status",
        "Item Name",
        "Item ID",
        "Option 1 Name",
        "Option 1 Value",
        "Option 2 Name",
        "Option 2 Value",
        "Auction Site",
        "Auction Buyer ID",
        "Auction Closing Date",
        "Shipping Address Line1",
        "Shipping Address Line2",
        "Shipping Address City",
        "Shipping Address State",
        "Shipping Address Zip",
        "Shipping Address Country",
        "Shipping Method",
        "Custom Field",
        "Billing Address Line1",
        "Billing Address Line2",
        "Billing Address City",
        "Billing Address State",
        "Billing Address Zip",
        "Billing Address Country",
        "Consumer ID",
        "First Name",
        "Last Name",
        "Consumer Business Name",
        "Card Type",
        "Payment Source",
        "Shipping Name",
        "Authorization Review Status",
        "Protection Eligibility",
        "Payment Tracking ID",
    ]

    @staticmethod
    def handle(path):
        obj = TrrFile(path)
        obj.parse()

    def __init__(self, path):
        self.path = path
        self.config = process.globals.get_config()
        self.crm = civicrm.civicrm.Civicrm(self.config.civicrm_db)

    def parse(self):
        # FIXME: encapsulation issues
        ppreport.read(self.path, self.VERSION, self.parse_line, self.column_headers)

    def parse_line(self, row):
        if row['Billing Address Line1']:
            addr_prefix = 'Billing Address '
        else:
            addr_prefix = 'Shipping Address '

        # FIXME: Accept an empty or malformed date, or keep the exception?
        out = {
            'gateway_txn_id': row['Transaction ID'],
            'date': ppreport.parse_date(row['Transaction Initiation Date']),
            'settled_date': ppreport.parse_date(row['Transaction Completion Date']),
            'gross': float(row['Gross Transaction Amount']) / 100.0,
            'currency': row['Gross Transaction Currency'],
            'gateway_status': row['Transactional Status'],
            'note': row['Transaction Note'],
            'email': row['Payer\'s Account ID'],

            'street_address': row[addr_prefix + 'Line1'],
            'supplemental_address_1': row[addr_prefix + 'Line2'],
            'city': row[addr_prefix + 'City'],
            'state_province': row[addr_prefix + 'State'],
            'postal_code': row[addr_prefix + 'Zip'],
            'country': row[addr_prefix + 'Country'],
        }

        if row['Fee Amount']:
            out['fee'] = float(row['Fee Amount']) / 100.0

            if row['Fee Currency'] and row['Gross Transaction Currency'] != row['Fee Currency']:
                raise RuntimeError("Failed to import because multiple currencies for one transaction is not handled.")

        if 'First Name' in row:
            out['first_name'] = row['First Name']

        if 'Last Name' in row:
            out['last_name'] = row['Last Name']

        if 'Payment Source' in row:
            out['payment_method'] = row['Payment Source']

        if 'Card Type' in row:
            out['payment_submethod'] = row['Card Type']

        # Look in all the places we might have stuck an order id
        for oid_field in ('Invoice ID', 'Transaction Subject', 'Custom Field'):
            if re.search('^[0-9]+(\\.[0-9]+)?$', row[oid_field]):
                out['order_id'] = row[oid_field]
                break

        if 'order_id' in out:
            # It can be the ct_id.attempt format
            out['contribution_tracking_id'] = out['order_id'].split('.')[0]

        event_type = row['Transaction Event Code'][0:3]

        queue = None
        if event_type in ('T00', 'T03', 'T05', 'T07', 'T22'):
            if row['Transaction Event Code'] == 'T0002':
                queue = 'recurring'
                out['txn_type'] = 'subscr_payment'
                out['subscr_id'] = row['PayPal Reference ID']
                if not out['subscr_id']:
                    raise Exception('Missing field subscr_id')
            elif row['Transaction Debit or Credit'] == 'DR':
                # sic: double-space is coming from the upstream
                log.info("-Debit\t{id}\t{date}\tPayment to".format(id=out['gateway_txn_id'], date=out['date']))
                # This payment is from us!  Do not send to the CRM.
                return
            else:
                queue = 'donations'
        elif event_type in ('T11', 'T12'):
            out['gateway_refund_id'] = out['gateway_txn_id']
            out['gross_currency'] = out['currency']

            if row['PayPal Reference ID Type'] == 'TXN':
                out['gateway_parent_id'] = row['PayPal Reference ID']

            if row['Transaction Event Code'] == 'T1106':
                out['type'] = 'reversal'
            elif row['Transaction Event Code'] == 'T1107':
                out['type'] = 'refund'
            elif row['Transaction Event Code'] == 'T1201':
                out['type'] = 'chargeback'
            else:
                log.info("-Unknown\t{id}\t{date}\t(Refundish type {type})".format(id=out['gateway_txn_id'], date=out['date'], type=row['Transaction Event Code']))
                return

            queue = 'refund'

        out['gateway'] = self.determine_gateway(row, queue)

        if not queue:
            log.info("-Unknown\t{id}\t{date}\t(Type {type})".format(id=out['gateway_txn_id'], date=out['date'], type=event_type))
            return

        if queue == 'donations' or queue == 'recurring':
            if self.crm.transaction_exists(gateway_txn_id=out['gateway_txn_id'], gateway=out['gateway']):
                log.info("-Duplicate\t{id}\t{date}\t{type}".format(id=out['gateway_txn_id'], date=row['Transaction Initiation Date'], type=queue))
                return

        if queue == 'recurring' and out['gateway'] == 'paypal_ec':
            # Some legacy recurring payments have been re-coded with I- subscription IDs, making them look like
            # EC donations. Check for the txn ID in legacy as well, to make sure we don't duplicate.
            if self.crm.transaction_exists(gateway_txn_id=out['gateway_txn_id'], gateway='paypal'):
                log.info("-Duplicate\t{id}\t{date}\t{type}".format(id=out['gateway_txn_id'], date=row['Transaction Initiation Date'], type=queue))
                return

        if queue == 'refund' and self.crm.transaction_refunded(gateway_txn_id=out['gateway_parent_id'], gateway=out['gateway']):
            log.info("-Duplicate\t{id}\t{date}\t{type}".format(id=out['gateway_txn_id'], date=row['Transaction Initiation Date'], type=queue))
            return

        if 'last_name' not in out and queue != 'refund':
            out['first_name'], out['last_name'] = paypal_api.PaypalApiClassic().fetch_donor_name(out['gateway_txn_id'])

        if self.config.no_thankyou:
            out['thankyou_date'] = 0

        log.info("+Sending\t{id}\t{date}\t{type}".format(id=out['gateway_txn_id'], date=row['Transaction Initiation Date'], type=queue))
        self.send(queue, out)

    def determine_gateway(self, row, queue):
        # FIXME: This is weasly, see that we're also sending the raw payment
        # source value as payment_method.
        if row['Payment Source'] == 'Express Checkout':
            return 'paypal_ec'

        # FIXME: tenuous logic here
        # For refunds, only paypal_ec sets the invoice ID
        if queue == 'refund' and row['Invoice ID']:
            return 'paypal_ec'

        # Skating further onto thin ice, we identify recurring version by
        # the first character of the subscr_id
        if queue == 'recurring' and row['PayPal Reference ID'][0] == 'I':
            return 'paypal_ec'

        return 'paypal'

    def send(self, queue_name, msg):
        if not self.redis:
            self.redis = queue.redis_wrap.Redis()

        self.redis.send(queue_name, msg)
