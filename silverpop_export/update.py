#!/usr/bin/env python

from csv import writer as CsvWriter
import errno
import re
import time
import os

from process.logging import Logger as log
from process.globals import load_config
load_config('silverpop_export')
from process.globals import config

from sftp.client import Client as SftpClient
from database.db import Connection as DbConnection, Query as DbQuery

import process.lock as lock

def load_queries(file):
    prefix = "-- Silverpop Export Script, %s" % file
    script_path = os.path.dirname(__file__)
    qbuf = [prefix]
    queries = []
    f = open( os.path.join( script_path, file ), 'r' )
    for line in f:
        line = line.rstrip()
        if line:
            qbuf.append(line)
            if line.endswith(';'):
                query = "\n".join(qbuf)
                # Do some database renaming
                query = re.sub(r"\scivicrm\.", " %s." % config.civicrm_db.db, query)
                query = re.sub(r"\sdrupal\.", " %s." % config.drupal_db.db, query)
                query = re.sub(r"\sgeonames\.", " %s." % config.geonames_db.db, query)
                queries.append(query)

                qbuf = [prefix]

    f.close()
    return queries


def run_queries(db, queries):
    i = 1
    for query in queries:
        log.info("Running query #%s" % i)
        db.execute(query)
        i += 1


def run_export_query(db, query, path, index):
    f = open(path, 'wb')
    w = CsvWriter(f)

    gen = db.execute_paged(query=query, pageIndex=index, pageSize=10000)

    # Make sure we've got the table headers
    try:
        first = gen.next()

        # Get the order of keys and sort them alphabetically so it doesn't come out as complete soup
        keys = sorted(first.keys())
        w.writerow(keys)
        w.writerow(order_keyed_row(keys, first))

        for row in gen:
            w.writerow(order_keyed_row(keys, row))
    except StopIteration:
        pass

    f.flush()
    f.close()


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
    global config
    log.info("Begin Silverpop Export")
    lock.begin()

    make_sure_path_exists(config.working_path)

    log.info("Loading update query set")
    update_queries = load_queries('update_table.sql')

    db = DbConnection(**config.silverpop_db)

    log.info("Starting update query run")
    run_queries(db, update_queries)

    log.info("Starting full data export")
    updatefile = 'DatabaseUpdate-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    exportq = DbQuery()
    exportq.tables.append('silverpop_export_view')
    exportq.columns.append('*')
    run_export_query(
        db,
        exportq,
        os.path.join(config['working_path'], updatefile),
        "ContactID"
    )

    log.info("Starting unsubscribe data export")
    unsubfile = 'Unsubscribes-' + time.strftime("%Y%m%d%H%M%S") + '.csv'
    exportq = DbQuery()
    exportq.tables.append('silverpop_export')
    exportq.columns.append('contact_id')
    exportq.columns.append('email')
    exportq.where.append('opted_out=1')
    run_export_query(
        db,
        exportq,
        os.path.join(config.working_path, unsubfile),
        "contact_id"
    )

    log.info("Uploading updates to silverpop")
    sftpc = SftpClient()
    sftpc.put(os.path.join(config.working_path, updatefile), updatefile)
    log.info("DB update done, now unsubscribes")
    sftpc.put(os.path.join(config.working_path, unsubfile), unsubfile)

    # Clean up after ourselves
    if config.days_to_keep_files:
        now = time.time()
        for f in os.listdir(config.working_path):
            path = os.path.join(config.working_path, f)
            if os.stat(path).st_mtime < (now - config.days_to_keep_files * 86400):
                if os.path.isfile(path):
                    log.info("Removing old file %s" % path)
                    os.remove(path)

    lock.end()
    log.info("End Silverpop Export")
