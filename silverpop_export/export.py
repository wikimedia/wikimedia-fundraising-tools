#!/usr/bin/env python

import errno
import os
import os.path
import time

from process.logging import Logger as log
from process.globals import load_config
load_config('silverpop_export')
from process.globals import config

from database.db import Connection as DbConnection, Query as DbQuery
import process.lock as lock
from sftp.client import Client as SftpClient
import unicode_csv_writer


def export_and_upload():
    log.info("Begin Silverpop Export")

    make_sure_path_exists(config.working_path)

    updatefile = os.path.join(
        config.working_path,
        'DatabaseUpdate-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )
    unsubfile = os.path.join(
        config.working_path,
        'Unsubscribes-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )

    export_data(output_path=updatefile)
    export_unsubscribes(output_path=unsubfile)
    upload([updatefile, unsubfile])
    rotate_files()

    log.info("End Silverpop Export")


def run_export_query(db=None, query=None, output=None, sort_by_index=None):
    """Export query results as a CSV file"""

    # Get a file-like object
    if not hasattr(output, 'write'):
        output = open(output, 'wb')

    w = unicode_csv_writer.UnicodeCsvWriter(output)

    gen = db.execute_paged(query=query, pageIndex=sort_by_index, pageSize=10000)

    # Make sure we've got the table headers
    try:
        first = gen.next()

        # Get the order of keys and sort them alphabetically so it doesn't come
        # out as complete soup
        keys = sorted(first.keys())
        w.writerow(keys)
        w.writerow(order_keyed_row(keys, first))

        for row in gen:
            w.writerow(order_keyed_row(keys, row))
    except StopIteration:
        pass

    output.flush()
    output.close()


def export_data(output_path=None):
    db = DbConnection(**config.silverpop_db)

    log.info("Starting full data export")
    exportq = DbQuery()
    exportq.tables.append('silverpop_export_view')
    exportq.columns.append('*')
    run_export_query(
        db=db,
        query=exportq,
        output=output_path,
        sort_by_index="ContactID"
    )


def export_unsubscribes(output_path=None):
    db = DbConnection(**config.silverpop_db)

    log.info("Starting unsubscribe data export")
    exportq = DbQuery()
    exportq.tables.append('silverpop_export')
    exportq.columns.append('contact_id')
    exportq.columns.append('email')
    exportq.where.append('opted_out=1')
    run_export_query(
        db=db,
        query=exportq,
        output=output_path,
        sort_by_index="contact_id"
    )


def upload(files=None):
    log.info("Uploading to silverpop")
    sftpc = SftpClient()
    for path in files:
        sftpc.put(path, os.path.basename(path))


def rotate_files():
    # Clean up after ourselves
    if config.days_to_keep_files:
        now = time.time()
        for f in os.listdir(config.working_path):
            path = os.path.join(config.working_path, f)
            if os.stat(path).st_mtime < (now - config.days_to_keep_files * 86400):
                if os.path.isfile(path):
                    log.info("Removing old file %s" % path)
                    os.remove(path)


def order_keyed_row(keys, row):
    result = []
    for key in keys:
        result.append(row[key])
    return result


def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


if __name__ == '__main__':
    lock.begin()

    export_and_upload()

    lock.end()
