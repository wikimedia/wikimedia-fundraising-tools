import datetime
import dateutil.parser
import dateutil.tz
import io
import logging
import os

from failmail.mailer import FailMailer
import csv

log = logging.getLogger(__name__)

dialect = dict(
    delimiter=',',
    quotechar='"'
)


def read(path, version, callback, column_headers, encoding):
    # Coerce to a list
    if not hasattr(version, 'extend'):
        version = [version]

    with io.open(path, 'r', encoding=encoding) as csvfile:
        plainreader = csv.reader(csvfile, **dialect)
        rownum = 1
        for row in plainreader:
            column_type = row[0]
            if column_type == 'RH':
                if int(row[4]) not in version:
                    raise RuntimeError("This file uses an unexpected format revision: {version}".format(version=row[4]))
            elif column_type == 'FH':
                pass
            elif column_type == 'SH':
                start_date, end_date = row[1:3]
                log.info("Report file covers date range {start} to {end}".format(start=start_date, end=end_date))
            elif column_type == 'CH':
                column_headers = ['Column Type'] + row[1:]
            elif column_type == 'SB':
                record = dict(list(zip(column_headers, row)))
                try:
                    callback(record)
                except Exception:
                    logme = {
                        'file': os.path.basename(path),
                        'row': rownum
                    }
                    for identifier in ['Transaction ID', 'Invoice ID', 'PayPal Reference ID', 'Subscription ID']:
                        if identifier in record:
                            logme[identifier] = record[identifier]

                    FailMailer.mail('BAD_AUDIT_LINE', data=logme, print_exception=True)
            elif column_type in ('SF', 'SC', 'RF', 'RC', 'FF'):
                pass
            else:
                raise RuntimeError("Unknown column type: {type}".format(type=column_type))

            rownum = rownum + 1


def parse_date(date_string):
    date_object = dateutil.parser.parse(date_string)
    utc = dateutil.tz.gettz('UTC')
    date_utc = date_object.astimezone(utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=utc)
    return int((date_utc - epoch).total_seconds())
