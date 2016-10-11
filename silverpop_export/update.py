#!/usr/bin/env python

import re
import os

from process.logging import Logger as log
from process.globals import load_config
load_config('silverpop_export')
from process.globals import config

from database.db import Connection as DbConnection
import export
import process.lock as lock


def load_queries(file):
    prefix = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;-- Silverpop Export Script, %s" % file
    script_path = os.path.dirname(__file__)
    qbuf = [prefix]
    queries = []
    f = open(os.path.join(script_path, file), 'r')
    for line in f:
        line = line.rstrip()
        if line:
            qbuf.append(line)
            if line.endswith(';'):
                query = "\n".join(qbuf)
                # Do some database renaming
                query = re.sub(r"\scivicrm\.", " %s." % config.civicrm_db.db, query)
                query = re.sub(r"\sdrupal\.", " %s." % config.drupal_db.db, query)
                query = re.sub(r"\slog_civicrm\.", " %s." % config.log_civicrm_db.db, query)
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


if __name__ == '__main__':
    global config
    log.info("Begin Silverpop Update")
    lock.begin()

    log.info("Loading update query set")
    update_queries = load_queries('update_table.sql')

    db = DbConnection(**config.silverpop_db)

    log.info("Starting update query run")
    run_queries(db, update_queries)

    export.export_and_upload()

    lock.end()
    log.info("End Silverpop Export")
