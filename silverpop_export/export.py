#!/usr/bin/env python3

import csv
import errno
import logging
import os
import time

import process.globals

from database.db import Connection as DbConnection, Query as DbQuery
import process.lock as lock


log = logging.getLogger(__name__)


def export_all():
    """
    Dump database contents to CSVs.
    """
    log.info("Begin Silverpop Export")
    config = process.globals.get_config()

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
    rotate_files()

    log.info("End Silverpop Export")


def run_export_query(db=None, query=None, output=None, sort_by_index=None):
    """Export query results as a CSV file"""

    # Get a file-like object
    if not hasattr(output, 'write'):
        output = open(output, 'w')

    w = csv.writer(output)

    gen = db.execute_paged(query=query, pageIndex=sort_by_index, pageSize=10000)

    # Make sure we've got the table headers
    try:
        first = next(gen)
        num_rows = 1

        # Get the order of keys and sort them alphabetically so it doesn't come
        # out as complete soup
        keys = sorted(first.keys())
        w.writerow(keys)
        w.writerow(order_keyed_row(keys, first))

        for row in gen:
            w.writerow(order_keyed_row(keys, row))
            num_rows += 1

    except StopIteration:
        pass

    output.flush()
    output.close()
    log.info("Wrote %d rows" % num_rows)


def export_data(output_path=None):
    config = process.globals.get_config()

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
    config = process.globals.get_config()

    db = DbConnection(**config.silverpop_db)

    log.info("Starting unsubscribe data export")
    exportq = DbQuery()
    exportq.tables.append('silverpop_excluded')
    exportq.columns.append('*')
    run_export_query(
        db=db,
        query=exportq,
        output=output_path,
        sort_by_index="id"
    )


def rotate_files():
    config = process.globals.get_config()

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
    process.globals.load_config('silverpop_export')

    lock.begin()

    export_all()

    lock.end()
