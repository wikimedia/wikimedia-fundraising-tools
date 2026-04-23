#!/usr/bin/env python3

"""
MediaWiki Donor Status Export

Exports donor status data from silverpop_export_view_full for sync to MediaWiki.
This module reads from the existing silverpop export views (built by the
silverpop_export module) and produces a simple CSV with:

  email_address, donor_status_id

Designed to run after the silverpop export has completed its table/view rebuild.
"""

import argparse
import csv
import logging
import os
import time
import subprocess
from datetime import datetime, timedelta

import process.globals
from database.db import Connection as DbConnection
import process.lock as lock

log = logging.getLogger(__name__)

EXPORT_QUERY = """
    SELECT
        -- e.ContactID AS contact_id,
        e.email,
        e.donor_status_id
        -- e.do_not_solicit
    FROM silverpop_export_view_full e
"""

EXPORT_QUERY_DELTA = """
    SELECT
        -- e.ContactID AS contact_id,
        e.email,
        e.donor_status_id
        -- e.do_not_solicit
    FROM silverpop_export_view_full e
    WHERE e.modified_date >= DATE_SUB(NOW(), INTERVAL %s DAY)
"""

EXPORT_QUERY_LIMIT = " LIMIT %s"

FRESHNESS_QUERY = """
    SELECT UPDATE_TIME
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
        AND table_name = 'silverpop_export'
"""


def export(days=None, limit=None):
    config = process.globals.get_config()
    db = DbConnection(**config.silverpop_db)
    check_data_freshness(db)

    os.makedirs(config.working_path, exist_ok=True)

    output_path = os.path.join(
        config.working_path,
        'MediaWikiDonorStatus-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )

    if days is not None:
        log.info("Exporting donor status (delta: last %s days)", days)
        query = EXPORT_QUERY_DELTA
        params = (days,)
    else:
        log.info("Exporting donor status (full)")
        query = EXPORT_QUERY
        params = None

    if limit is not None:
        query += EXPORT_QUERY_LIMIT
        params = (params or ()) + (limit,)

    try:
        results = db.execute(query, params)

        fieldnames = ['email', 'donor_status_id']
        # fieldnames += ['contact_id', 'do_not_solicit']

        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            num_rows = 0
            for row in results:
                writer.writerow(row)
                num_rows += 1

        log.info("Wrote %d rows to %s", num_rows, output_path)

        if 'age_identity_file' in config:
            output_path = encrypt_file(output_path, config.age_identity_file)

        if 'sftp' in config:
            upload(output_path)

        return output_path
    finally:
        db.db_conn.close()


def check_data_freshness(db, max_staleness_hours=36):
    rows = db.execute(FRESHNESS_QUERY)
    row = next(iter(rows), None)
    if row is None or row['UPDATE_TIME'] is None:
        raise RuntimeError("Cannot determine silverpop_export update time")
    age = datetime.now() - row['UPDATE_TIME']
    if age > timedelta(hours=max_staleness_hours):
        raise RuntimeError(
            f"silverpop_export data is stale: last updated {age} ago "
            f"(max allowed: {max_staleness_hours}h)"
        )


def upload(path):
    # Lazy import: paramiko is not in the test requirements
    # SftpClient picks up the sftp config via process.globals.get_config()
    from sftp.client import Client as SftpClient
    log.info("Uploading %s via SFTP", os.path.basename(path))
    sftpc = SftpClient()
    sftpc.put_atomic(path, os.path.basename(path))
    sftpc.close()


def encrypt_file(input_path, identity_path):
    enc_path = input_path + '.age'
    subprocess.run(
        [
            'age',
            '-e',
            '-i', identity_path,
            '-o', enc_path,
            input_path
        ],
        check=True,
    )
    os.remove(input_path)
    log.info("Encrypted output: %s", enc_path)
    return enc_path


if __name__ == '__main__':
    config = process.globals.load_config('mediawiki_donor_export')

    parser = argparse.ArgumentParser(
        description='Export donor status from silverpop views for MediaWiki sync'
    )
    parser.add_argument(
        '-d', '--days', type=int, default=None,
        help='Export contacts modified in the last N days (default: full export)'
    )
    parser.add_argument(
        '-l', '--limit', type=int, default=None,
        help='Limit the number of rows exported (useful for testing)'
    )

    args = parser.parse_args()

    days = args.days
    if days is None and 'offset_in_days' in config:
        days = config.offset_in_days

    lock.begin()
    export(days=days, limit=args.limit)
    lock.end()
