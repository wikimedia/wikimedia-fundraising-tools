"""Parser for Transaction Reconciliation files

See EMIS User Guide V9 January 2014.pdf
"""

from calendar import timegm as to_timestamp
from datetime import datetime
import os.path
import re
import struct

from process.logging import Logger as log
from process.globals import config

from civicrm.civicrm import Civicrm
from queue.stomp_wrap import Stomp
import reference_data

class FixedRecordType(object):
    def __init__(self, record_type, *field_segments):
        self.record_type = record_type
        self.fields = []
        for segment in field_segments:
            self.fields.extend(segment)

class TransactionReconciliationFile(object):
    filename_re = r"^MA\.PISCESSW\.#M\.RECON\..*"

    stomp = None

    row_header_segment = [
        ("record_type", 2),
        ("sequence_no", 8),
    ]

    credit_debit_summary_segment = [
        ("accepted_debits", 11),
        ("rejected_debits", 11),
        ("pending_debits", 11),
        ("accepted_credits", 11),
        ("rejected_credits", 11),
        ("pending_credits", 11),
        ("accepted_debits_count", 7),
        ("rejected_debits_count", 7),
        ("pending_debits_count", 7),
        ("accepted_credits_count", 7),
        ("rejected_credits_count", 7),
        ("pending_credits_count", 7),
    ]

    file_header = FixedRecordType("00",
        [
            ("record_type", 2),
            ("sequence_no", 8),
            ("file_id", 13),
            ("count", 7),
        ],
        credit_debit_summary_segment,
        [
            ("file_creation_date", 6),
            ("file_sequence_number", 3),
            ("site_id", 3),
        ]
    )

    reconciliation_merchant_company_header = FixedRecordType("05",
        [
            ("record_type", 2),
            ("sequence_no", 8),
            ("company_number", 13),
            ("count", 7),
        ],
        credit_debit_summary_segment
    )

    reconciliation_merchant_outlet = FixedRecordType("10",
        [
            ("record_type", 2),
            ("sequence_no", 8),
            ("merchant_id", 13),
            ("trading_day", 6),
            ("processing_date", 6),
        ],
        credit_debit_summary_segment
    )

    reconciliation_transaction_data = FixedRecordType("15", [
        ("record_type", 2),
        ("sequence_no", 8),
        ("pan", 19),
        ("expiry_date", 4),
        ("transaction_value", 11),
        ("transaction_date", 6),
        ("transaction_time", 6),
        ("transaction_type", 1),
        ("transaction_source", 1),
        ("receipt_number", 6),
        ("status", 1),
        ("reserved1", 2),
        ("local_value", 9),
        ("local_currency_code", 3),
        ("local_exponent", 1),
        ("settlement_value", 9),
        ("settlement_currency_code", 3),
        ("settlement_exponent", 1),
        ("acquired_processed_indicator", 1),
        ("card_type", 5),
    ])

    reconciliation_transaction_supplementary_data = FixedRecordType("16", [
        ("record_type", 2),
        ("sequence_no", 8),
        ("auth_code", 6),
        ("auth_method", 1),
        ("card_issue_number", 2),
        ("card_start_date", 4),
        ("cash_amount", 7),
        ("originators_transaction_reference", 20),
        ("ticket_number", 14),
    ])

    known_record_types = [
        file_header,
        reconciliation_merchant_company_header,
        reconciliation_merchant_outlet,
        reconciliation_transaction_data,
        reconciliation_transaction_supplementary_data,
    ]

    @staticmethod
    def is_mine(path):
        filename = os.path.basename(path)
        return re.match(TransactionReconciliationFile.filename_re, filename)

    @staticmethod
    def handle(path):
        obj = TransactionReconciliationFile(path)
        obj.parse()

    def __init__(self, path):
        self.path = path
        self.crm = Civicrm(config.civicrm_db)

        self.pending_data = None
        self.pending_supplemental_data = None

    def parse(self):
        """Parse the file"""
        self.file = file(self.path)
        for line in self.file:
            self.parse_line(line.rstrip("\r\n"))

        self.flush_data()

    def parse_line(self, line):
        """Parse one line and send it to the appropriate queue

        There is a crazy thing happening here where we need to coordinate
        sequential lines, and merge supplemental records into the main transaction
        data.  See add_transaction_data and add_supplementary_data.
        """
        # Peek at row header to determine its type
        row_info = unpack_fixed_width_line(self.row_header_segment, line[:10])

        # Find the corresponding line format and parse the contents
        record = None
        for record_type in self.known_record_types:
            if record_type.record_type == row_info["record_type"]:
                record = unpack_fixed_width_line(record_type.fields, line)

        if not record:
            raise RuntimeError("Unknown record type {type} while processing {path}, aborting!".format(type=row_info["record_type"], path=self.path))

        # Dispatch to a handler
        if record["record_type"] == self.reconciliation_transaction_data.record_type:
            self.add_transaction_data(record)
        elif record["record_type"] == self.reconciliation_transaction_supplementary_data.record_type:
            self.add_supplementary_data(record)
        else:
            # ignore other crap.
            # TODO: assertions for checksummy things built into the file
            pass

    def send(self, queue, msg):
        """Send over the wire"""
        if not self.stomp:
            self.stomp = Stomp()

        self.stomp.send(queue, msg)

    def add_transaction_data(self, record):
        self.flush_data()
        self.pending_data = record

    def add_supplementary_data(self, record):
        if not self.pending_data:
            raise RuntimeError("Cannot eat supplementary transaction data because there no unconsumed base data. Line {line}".format(line=record["sequence_no"]))

        if self.pending_supplemental_data:
            raise RuntimeError("Cannot eat supplementary data because there is already unconsumed supplemental data. Line {line}".format(line=record["sequence_no"]))

        self.pending_supplemental_data = record

    def flush_data(self):
        if self.pending_data:
            self.send_transaction()

    def send_transaction(self):
        record = self.pending_data

        # Verify that the data and supplemental data are a pair
        if self.pending_supplemental_data:
            if int(self.pending_supplemental_data["sequence_no"]) != int(self.pending_data["sequence_no"]) + 1:
                raise RuntimeError("Mismatched data and supplemental data!")
            record.update(self.pending_supplemental_data)

        self.normalize_and_send(record)

        self.pending_data = None
        self.pending_supplemental_data = None

    def normalize_and_send(self, record):
        """Transform the record into a WMF queue message

        See https://wikitech.wikimedia.org/wiki/Fundraising/Queue_messages"""

        msg = {}

        if record["transaction_type"] == "0":
            queue = "donations"
        elif record["transaction_type"] == "5":
            queue = "refund"
        else:
            raise RuntimeError("Don't know how to handle transaction type {type}.".format(type=record["transaction_type"]))

        msg["date"] = to_timestamp(datetime.strptime(record["transaction_date"] + record["transaction_time"], "%d%m%y%H%M%S").utctimetuple())
        iso_date = datetime.fromtimestamp(msg["date"]).isoformat()

        msg["gateway"] = "worldpay"

        # FIXME: is this the CustomerId or what?
        if "originators_transaction_reference" in record:
            msg["gateway_txn_id"] = record["originators_transaction_reference"].strip()
        else:
            raise RuntimeError("We're gonna die: no gateway_txn_id available.")

        # The default currency is GBP, don't make me explain why the amount
        # comes from a different field when currency != GBP :(
        if record["local_currency_code"].strip():
            msg["currency"] = record["local_currency_code"]
            msg["gross"] = int(record["local_value"]) * exponent_to_multiplier(record["local_exponent"])
        else:
            msg["currency"] = "GBP"
            msg["gross"] = int(record["transaction_value"]) * exponent_to_multiplier(2)

        if queue == "refund":
            msg["gross_currency"] = msg["currency"]
            msg["gateway_parent_id"] = msg["gateway_txn_id"]
            # FIXME: chargeback vs refund info is not available in this file.
            msg["type"] = "refund"
            log.info("+Sending\t{id}\t{date}\t{type}".format(id=msg["gateway_parent_id"], date=iso_date, type=msg["type"]))
            self.send(queue, msg)
            return

        if self.crm.transaction_exists(gateway_txn_id=msg["gateway_txn_id"], gateway="worldpay"):
            log.info("-Duplicate\t{id}\t{date}\t{type}".format(id=msg["gateway_txn_id"], date=iso_date, type=queue))
            return

        # Switch behavior depending on the status.  We only like "accepted" transactions.
        status = record["status"].strip()
        if status == "P":
            log.info("-Pending\t{id}\t{date}\t{type}".format(id=msg["gateway_txn_id"], date=iso_date, type=queue))
            return
        elif status == "R":
            log.info("-Rejection\t{id}\t{date}\t{type}".format(id=msg["gateway_txn_id"], date=iso_date, type=queue))
            return
        elif status != "A":
            raise RuntimeError("Unknown gateway status: {code}".format(code=status))

        # Include settlement details if they are available.
        if record["settlement_value"].strip():
            if record["settlement_currency_code"].strip():
                msg["settlement_currency"] = record["settlement_currency_code"]
            else:
                msg["settlement_currency"] = "GBP"
            msg["settlement_amount"] = int(record["settlement_value"]) * exponent_to_multiplier(record["settlement_exponent"])

        msg["email"] = "nobody@wikimedia.org"
        msg["payment_method"] = "cc"
        msg["payment_submethod"] = reference_data.decode_card_type(record["card_type"].strip())

        # custom values
        msg["raw_card_type"] = record["card_type"].strip()

        log.info("+Sending\t{id}\t{date}\t{type}".format(id=msg["gateway_txn_id"], date=iso_date, type=queue))
        self.send(queue, msg)

    def normalize_transaction(self, record):
        """Transform a raw reconciliation record into a donation queue message"""
        # TODO
        return record

def unpack_fixed_width_line(fields, line):
    fmtstring = " ".join(["{}s".format(f[1]) for f in fields])
    parser = struct.Struct(fmtstring)
    line = line.ljust(parser.size)
    raw = parser.unpack(line)
    return dict(zip([ f[0] for f in fields ], raw))

def exponent_to_multiplier(exponent):
    """Convert an exponent to a multiplier

    The exponent defines how many digits are "minor units" of the currency, so USD
    for example has an exponent of 2.  Our queue consumer assumes that amounts are
    always given in major units, so we produce a multiplier here which will
    convert from the WorldPay amount, formatted in minor units, to the expected amount.

    For example, a currency with 3 digits of minor units should be multipled by 0.001
    before sending over the queue."""

    if not exponent.strip():
        # The default is GBP, which have an exponent of 2.
        exponent = 2

    return pow(10, 0 - int(exponent))
