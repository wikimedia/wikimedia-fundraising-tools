#!/usr/bin/env python3

import logging
import re
import os

import process.globals

from database.db import Connection as DbConnection
from silverpop_export import export
import process.lock as lock

log = logging.getLogger(__name__)


def load_queries(file):
    # TODO: Reuse database.db.load_queries

    config = process.globals.get_config()

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
    """
    Build silverpop_export database from CiviCRM.
    """
    i = 1
    for query in queries:
        no_prefix = query[query.index("\n") + 1:]
        info = (i, no_prefix[:80])
        log.info("Running query #%s: %s" % info)
        db.execute(query)
        i += 1


def updateAll():
    log.info("Loading schema update set ")
    drop_queries = load_queries('drop_schema.sql')
    log.info("Loading schema update set ")
    rebuild_queries = load_queries('rebuild_schema.sql')
    log.info("Loading schema update set ")
    # Probably this should be in the rebuild but leaving for later to keep commits readable.
    language_queries = load_queries('silverpop_countrylangs.sql')
    log.info("Loading update query set")
    update_queries = load_queries('update_table.sql')
    log.info("Loading update query set")
    update_suppression_queries = load_queries('update_suppression_list.sql')
    db = DbConnection(**config.silverpop_db)
    log.info("Dropping schema (temporary step)")
    run_queries(db, drop_queries)
    log.info("Rebuilding schema (temporary step)")
    run_queries(db, rebuild_queries)
    log.info("Rebuilding language table.")
    run_queries(db, language_queries)
    log.info("Starting update query run")
    run_queries(db, update_queries)
    log.info("Starting update query run")
    run_queries(db, update_suppression_queries)
    export.export_all()


if __name__ == '__main__':
    config = process.globals.load_config('silverpop_export')

    log.info("Begin Silverpop Update")
    lock.begin()

    updateAll()

    lock.end()
    log.info("End Silverpop Export")
