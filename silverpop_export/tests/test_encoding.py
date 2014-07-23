#/usr/bin/env python
# -*- coding: utf-8 -*-
"""TODO: refactor as a unit test"""

import sys

from process.logging import Logger as log
from process.globals import load_config
load_config('silverpop_export')
from process.globals import config

import database.db
import silverpop_export.update

db = database.db.Connection(**config.silverpop_db)

query = database.db.Query()
query.tables.append('silverpop_export_view')
query.columns.append('*')

silverpop_export.update.run_export_query(
    db = db,
    query = query,
    output = sys.stdout,
    sort_by_index = "ContactID"
)
