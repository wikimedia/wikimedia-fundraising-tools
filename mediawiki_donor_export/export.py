#!/usr/bin/env python3

"""
MediaWiki Donor Status Export

Exports donor status data from silverpop_export_view_full for sync to MediaWiki.
This module reads from the existing silverpop export views (built by the
silverpop_export module) and produces a simple CSV with:

  contact_id, email_address, donor_status_id

Designed to run after the silverpop export has completed its table/view rebuild.
"""

import argparse
import csv
import logging
import os
import time

import process.globals
from database.db import Connection as DbConnection
import process.lock as lock

log = logging.getLogger(__name__)

EXPORT_QUERY = """
    SELECT
        e.ContactID AS contact_id,
        e.email AS email_address,
        e.donor_status_id
    FROM silverpop_export_view_full e
"""

EXPORT_QUERY_DELTA = """
    SELECT
        e.ContactID AS contact_id,
        e.email AS email_address,
        e.donor_status_id
    FROM silverpop_export_view_full e
    WHERE e.modified_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
"""


def export(days=None):
    config = process.globals.get_config()
    db = DbConnection(**config.silverpop_db)

    os.makedirs(config.working_path, exist_ok=True)

    output_path = os.path.join(
        config.working_path,
        'MediaWikiDonorStatus-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )

    if days is not None:
        log.info("Exporting donor status (delta: last %s days)", days)
        results = db.execute(EXPORT_QUERY_DELTA, (days,))
    else:
        log.info("Exporting donor status (full)")
        results = db.execute(EXPORT_QUERY)

    fieldnames = ['contact_id', 'email_address', 'donor_status_id']

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        num_rows = 0
        for row in results:
            writer.writerow(row)
            num_rows += 1

    log.info("Wrote %d rows to %s", num_rows, output_path)
    return output_path


if __name__ == '__main__':
    config = process.globals.load_config('mediawiki_donor_export')

    parser = argparse.ArgumentParser(
        description='Export donor status from silverpop views for MediaWiki sync'
    )
    parser.add_argument(
        '-d', '--days', type=int, default=None,
        help='Export contacts modified in the last N days (default: full export)'
    )
    args = parser.parse_args()

    days = args.days
    if days is None and 'offset_in_days' in config:
        days = config.offset_in_days

    lock.begin()
    export(days=days)
    lock.end()
