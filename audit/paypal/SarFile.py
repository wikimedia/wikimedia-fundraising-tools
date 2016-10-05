'''Parser for Paypal Subscription Agreement Report files

See https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_SubscribeAgmntRprt.pdf
'''

from process.logging import Logger as log
from process.globals import config
from queue.redis_wrap import Redis
import ppreport
from civicrm.civicrm import Civicrm

class SarFile(object):
    VERSION=2
    redis = None
    column_headers = [
        "Column Type",
        "Subscription ID",
        "Subscription Action Type",
        "Subscription Currency",
        "Subscription Creation Date",
        "Subscription Period 1",
        "Period 1 Amount",
        "Subscription Period 2",
        "Period 2 Amount",
        "Subscription Period 3",
        "Period 3 Amount",
        "Recurring",
        "Recurrence number",
        "Subscription Payer PayPal Account ID",
        "Subscription Payer email address",
        "Subscription Payer Name",
        "Subscription Payer Business Name",
        "Shipping Address Line1",
        "Shipping Address City",
        "Shipping Address State",
        "Shipping Address Zip",
        "Shipping Address Country",
        "Subscription Description",
        "Subscription Memo",
        "Subscription Custom Field",
    ]

    @staticmethod
    def handle(path):
        obj = SarFile(path)
        obj.parse()

    def __init__(self, path):
        self.path = path
        self.crm = Civicrm(config.civicrm_db)

    def parse(self):
        ppreport.read(self.path, self.VERSION, self.parse_line, self.column_headers)

    def parse_line(self, row):
        required_fields = [
            "Period 3 Amount",
            "Subscription Currency",
            "Subscription ID",
            "Subscription Payer Name",
            "Subscription Period 3",
        ]

        missing_fields = []
        for field in required_fields:
            if not field in row or row[field] == '':
                missing_fields.append(field)
        if missing_fields:
            raise RuntimeError("Message is missing some important fields: [{fields}]".format(fields=", ".join(missing_fields)))

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

        # FIXME what historical evil caused...
        if row['Subscription Period 3'] != "1 M":
            raise RuntimeError("Unknown subscription period {period}".format(period=row['Subscription Period 3']))

        if row['Subscription Action Type'] == 'S0000':
            out['txn_type'] = 'subscr_signup'
            if self.crm.subscription_exists(out['subscr_id']):
                log.info("-Duplicate\t{id}\t{date}\tsubscr_signup".format(id=out['subscr_id'], date=out['subscr_date']))
                return
        elif row['Subscription Action Type'] == 'S0100':
            log.info("-Ignored\t{id}\t{date}\tsubscr_modify".format(id=out['subscr_id'], date=out['subscr_date']))
            return
        elif row['Subscription Action Type'] == 'S0200':
            out['txn_type'] = 'subscr_cancel'
            out['cancel_date'] = out['subscr_date']
        elif row['Subscription Action Type'] == 'S0300':
            out['txn_type'] = 'subscr_eot'

        if config.no_thankyou:
            out['thankyou_date'] = 0

        log.info("+Sending\t{id}\t{date}\t{type}".format(id=out['subscr_id'], date=out['subscr_date'], type=out['txn_type']))
        self.send(out)

    def send(self, msg):
        if not self.redis:
            self.redis = Redis()

        self.redis.send('recurring', msg)
