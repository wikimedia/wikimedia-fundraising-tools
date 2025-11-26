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
prometheus_file = None


def export_all():
    """
    Dump database contents to CSVs.
    """
    global prometheus_file

    log.info("Begin Silverpop Export")
    config = process.globals.get_config()

    make_sure_path_exists(config.working_path)

    if 'prometheus_path' in config and config.prometheus_path is not None:
        prometheus_file = open(config.prometheus_path, 'w')
        log.debug("Opened prometheus_file %s" % config.prometheus_path)

    log.info("Writing files to directory " + config.working_path)

    updatefile = os.path.join(
        config.working_path,
        'DatabaseUpdate-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )
    unsubfile = os.path.join(
        config.working_path,
        'Unsubscribes-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )
    optoutfile = os.path.join(
        config.working_path,
        'Optout-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )
    checksumemailsfile = os.path.join(
        config.working_path,
        'ChecksumEmails-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    )

    export_data(output_path=updatefile)
    export_unsubscribes(output_path=unsubfile, metric_phase="Unsubscribes")
    export_unsubscribes(output_path=optoutfile, metric_phase="Optout")
    export_checksum_email(output_path=checksumemailsfile)

    if prometheus_file is not None:
        prometheus_file.flush()
        prometheus_file.close()

    rotate_files()

    log.info("End Silverpop Export")


def run_export_query(db=None, query=None, output=None, sort_by_index=None, metric_phase=None):
    """Export query results as a CSV file"""
    global prometheus_file

    # Get a file-like object
    if not hasattr(output, 'write'):
        output = open(output, 'w')

    w = csv.writer(output)

    gen = db.execute_paged(query=query, pageIndex=sort_by_index, pageSize=10000)
    num_rows = 0

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
    if metric_phase is not None and prometheus_file is not None:
        prometheus_file.write("acoustic_export_count{phase=\"%s\"} %d\n" % (metric_phase, num_rows))


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
        sort_by_index="ContactID",
        metric_phase="DatabaseUpdate"
    )


def export_unsubscribes(output_path=None, metric_phase=None):
    config = process.globals.get_config()

    db = DbConnection(**config.silverpop_db)

    log.info("Starting unsubscribe data export")
    exportq = DbQuery()
    exportq.tables.append('silverpop_excluded_utf8')
    exportq.columns.append('*')
    run_export_query(
        db=db,
        query=exportq,
        output=output_path,
        sort_by_index="email",
        metric_phase=metric_phase
    )


def export_checksum_email(output_path=None):
    config = process.globals.get_config()

    db = DbConnection(**config.silverpop_db)

    log.info("Starting email with correspondent checksum data export")
    exportq = DbQuery()
    exportq.tables.append('silverpop_export_checksum_email')
    exportq.columns.append('*')
    run_export_query(
        db=db,
        query=exportq,
        output=output_path,
        sort_by_index="email",
        metric_phase="ChecksumEmails"
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
