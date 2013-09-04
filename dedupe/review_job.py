from process.globals import config
from database import db

class ReviewJob(object):
    def __init__(self, name):
        self.name = name

        sql = "INSERT INTO donor_autoreview_job SET name = %s"
        dbc = db.get_db(config.drupal_schema)
        dbc.execute(sql, (name, ))
        self.id = dbc.last_insert_id()
