from process.globals import config
from database import db


class Tag(object):
    cache = {}

    @staticmethod
    def get(name):
        if name not in Tag.cache:
            Tag.cache[name] = Tag(name)

        return Tag.cache[name]

    def __init__(self, name):
        self.name = name

        sql = "SELECT id FROM civicrm_tag WHERE name = %s"
        results = list(db.get_db(config.civicrm_schema).execute(sql, (name, )))
        if not results:
            raise RuntimeError("Db schema missing tag: " + name)
        self.id = results[0]['id']
