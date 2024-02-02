'''Parser for Paypal Subscription Agreement Report files

See https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_SubscribeAgmntRprt.pdf
'''

import logging

import process.globals
import frqueue.redis_wrap
from . import ppreport

import civicrm.civicrm

log = logging.getLogger(__name__)


class SarFile(object):
    VERSION = 2
    FILE_ENCODING = 'utf_16'
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
        self.config = process.globals.get_config()
        self.crm = civicrm.civicrm.Civicrm(self.config.civicrm_db)

    def parse(self):
        ppreport.read(self.path, self.VERSION, self.parse_line, self.column_headers, self.FILE_ENCODING)

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
            if field not in row or row[field] == '':
                missing_fields.append(field)
        if missing_fields:
            raise RuntimeError("Message is missing some important fields: [{fields}]".format(fields=", ".join(missing_fields)))

        names = row['Subscription Payer Name'].split(' ')
        date = ppreport.parse_date(row['Subscription Creation Date'])

        out = {
            'subscr_id': row['Subscription ID'],
            'currency': row['Subscription Currency'],
            'gross': float(row['Period 3 Amount']) / 100,
            'email': row['Subscription Payer email address'],
            'first_name': names[0],
            'last_name': " ".join(names[1:]),
            'street_address': row['Shipping Address Line1'],
            'city': row['Shipping Address City'],
            'postal_code': row['Shipping Address Zip'],
            'state_province': row['Shipping Address State'],
            'country': row['Shipping Address Country'],
            'gateway': 'paypal',  # TODO: Express checkout
        }

        # FIXME what historical evil caused...
        if row['Subscription Period 3'] != "1 M":
            raise RuntimeError("Unknown subscription period {period}".format(period=row['Subscription Period 3']))
        else:
            out['frequency_interval'] = '1'
            out['frequency_unit'] = 'month'

        log_params = {
            'id': out['subscr_id'],
            'date': row['Subscription Creation Date'],
        }

        if row['Subscription Action Type'] == 'S0000':
            out['txn_type'] = 'subscr_signup'
            out['start_date'] = date
            out['create_date'] = date
            if self.crm.subscription_exists(out['subscr_id']):
                log.info("-Duplicate\t{id}\t{date}\tsubscr_signup".format(**log_params))
                return
        elif row['Subscription Action Type'] == 'S0100':
            log.info("-Ignored\t{id}\t{date}\tsubscr_modify".format(**log_params))
            return
        elif row['Subscription Action Type'] == 'S0200':
            if not self.crm.subscription_exists(out['subscr_id']):
                log.info("-Duplicate\t{id}\t{date}\tsubscr_cancel".format(**log_params))
                return
            out['txn_type'] = 'subscr_cancel'
            out['cancel_date'] = date
        elif row['Subscription Action Type'] == 'S0300':
            out['txn_type'] = 'subscr_eot'

        if self.config.no_thankyou:
            out['no_thank_you'] = 'Audit configured not to send TY messages'

        log_params['type'] = out['txn_type']
        log.info("+Sending\t{id}\t{date}\t{type}".format(**log_params))
        self.send(out)

    def send(self, msg):
        if not self.redis:
            self.redis = frqueue.redis_wrap.Redis()

        self.redis.send('recurring', msg)
