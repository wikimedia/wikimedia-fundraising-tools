'''Parser for Paypal Subscription Agreement Report files

See https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_SubscribeAgmntRprt.pdf
'''

from process.logging import Logger as log
from queue.stomp_wrap import Stomp
import ppreport
from civicrm.civicrm import Civicrm

class SarFile(object):
    VERSION=2
    stomp = None

    @staticmethod
    def handle(path):
        obj = SarFile(path)
        obj.parse()

    def __init__(self, path):
        self.path = path
        self.crm = Civicrm(config.civicrm_db)

    def parse(self):
        ppreport.read(self.path, self.VERSION, self.parse_line)

    def parse_line(self, row):
        names = row['Subscription Payer Name'].split(' ')

        out = {
            'subscr_id': row['Subscription ID'],
            'mc_currency': row['Subscription Currency'],
            'mc_amount3': float(row['Period 3 Amount']) / 100,
            'period3': row['Subscription Period 3'],
            'subscr_date': row['Subscription Creation Date'],
            'payer_email': row['Subscription Payer email address'],
            'first_name': names[0],
            'last_name': " ".join(names[1:]),
            'address_street': row['Shipping Address Line1'],
            'address_city': row['Shipping Address City'],
            'address_zip': row['Shipping Address Zip'],
            'address_state': row['Shipping Address State'],
            'address_country_code': row['Shipping Address Country'],
            'gateway': 'paypal',
        }

        if row['Subscription Period 3'] != "1 M":
            raise RuntimeError("Unknown subscription period {period}".format(period=row['Subscription Period 3']))

        if row['Subscription Action Type'] == 'S0000':
            out['txn_type'] = 'subscr_signup'
            if self.crm.subscription_exists(out['subscr_id']):
                log.info("Skipping duplicate subscription signup.")
                return
        elif row['Subscription Action Type'] == 'S0100':
            log.info("Ignoring subscription modification")
        elif row['Subscription Action Type'] == 'S0200':
            out['txn_type'] = 'subscr_cancel'
        elif row['Subscription Action Type'] == 'S0300':
            out['txn_type'] = 'subscr_eot'

        self.send(out)

    def send(self, msg):
        if not self.stomp:
            self.stomp = Stomp()

        self.stomp.send(msg, 'recurring')
