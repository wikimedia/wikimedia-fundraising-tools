from process.globals import config
from database import db

class Action(object):
    cache = {}

    @staticmethod
    def get(name):
        if name not in Action.cache:
            Action.cache[name] = Action(name)

        return Action.cache[name]

    def __init__(self, name):
        self.name = name

        sql = "SELECT id FROM donor_review_action WHERE name = %s"
        results = list(db.get_db(config.drupal_schema).execute(sql, (name, )))
        if not results:
            raise RuntimeError("Db schema missing action: " + name)
        self.id = results[0]['id']
