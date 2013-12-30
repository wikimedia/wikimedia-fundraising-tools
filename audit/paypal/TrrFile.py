'''Parser for Transaction Detail Report files

See https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_Gen_TransactionDetailReport.pdf
'''

from process.logging import Logger as log
from process.globals import config
from queue.stomp_wrap import Stomp
import ppreport

from civicrm.civicrm import Civicrm
from paypal_api import PaypalApiClassic

class TrrFile(object):
    VERSION = [4, 8]
    stomp = None

    @staticmethod
    def handle(path):
        obj = TrrFile(path)
        obj.parse()

    def __init__(self, path):
        self.path = path
        self.crm = Civicrm(config.civicrm_db)

    def parse(self):
        ppreport.read(self.path, self.VERSION, self.parse_line)

    def parse_line(self, row):
        if row['Billing Address Line1']:
            addr_prefix = 'Billing Address '
        else:
            addr_prefix = 'Shipping Address '

        out = {
            'gateway_txn_id': row['Transaction ID'],
            'date': row['Transaction Initiation Date'],
            'settled_date': row['Transaction Completion Date'],
            'gross': float(row['Gross Transaction Amount']) / 100.0,
            'currency': row['Gross Transaction Currency'],
            'gateway_status': row['Transactional Status'],
            'gateway': 'paypal',
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

        if row['PayPal Reference ID Type'] == 'SUB':
            out['subscr_id'] = row['PayPal Reference ID']

        event_type = row['Transaction Event Code'][0:3]

        queue = None
        if event_type in ('T00', 'T03', 'T05', 'T07', 'T22'):
            if row['Transaction Event Code'] == 'T0002':
                queue = 'recurring'
                out = self.normalize_recurring(out)
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

        if not queue:
            log.info("-Unknown\t{id}\t{date}\t(Type {type})".format(id=out['gateway_txn_id'], date=out['date'], type=event_type))
            return

        if self.crm.transaction_exists(gateway_txn_id=out['gateway_txn_id'], gateway='paypal'):
            log.info("-Duplicate\t{id}\t{date}\t{type}".format(id=out['gateway_txn_id'], date=row['Transaction Initiation Date'], type=queue))
            return

        if 'last_name' not in out and queue != 'refund':
            out['first_name'], out['last_name'] = self.fetch_donor_name(out['gateway_txn_id'])

        # Magic to suppress a Thank-You email. TODO: flag in a way that is more discoverable
        out['thankyou_date'] = 0

        log.info("+Sending\t{id}\t{date}\t{type}".format(id=out['gateway_txn_id'], date=out['date'], type=queue))
        self.send(queue, out)

    def send(self, queue, msg):
        if not self.stomp:
            self.stomp = Stomp()

        self.stomp.send(msg, queue)

    def normalize_recurring(self, msg):
        'Synthesize a raw PayPal message'

        if 'fee' not in msg:
            msg['fee'] = 0

        out = {
            'gateway': 'paypal',
            'txn_type': 'subscr_payment',
            'gateway_txn_id': msg['gateway_txn_id'],
            'txn_id': msg['gateway_txn_id'],
            'subscr_id': msg['subscr_id'],
            'payment_date': msg['date'],
            'payer_email': msg['email'],
            'mc_currency': msg['currency'],
            'mc_gross': msg['gross'],
            'mc_fee': msg['fee'],
            'address_street': "\n".join([msg['street_address'], msg['supplemental_address_1']]),
            'address_city': msg['city'],
            'address_zip': msg['postal_code'],
            'address_state': msg['state_province'],
            'address_country_code': msg['country'],
        }

        return out

    def fetch_donor_name(self, txn_id):
        api = PaypalApiClassic()
        response = api.call('GetTransactionDetails', TRANSACTIONID=txn_id)
        if 'FIRSTNAME' not in response:
            raise RuntimeError("Failed to get transaction details for {id}, repsonse: {response}".format(id=txn_id, response=response))
        return (response['FIRSTNAME'][0], response['LASTNAME'][0])
