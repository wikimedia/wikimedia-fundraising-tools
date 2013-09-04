'''
Mysql wrapper which allows query composition
'''
import MySQLdb as Dbi
import atexit

from process.globals import config

class Connection(object):
    def __init__(self, host=None, user=None, passwd=None, db=None, debug=False):
        self.db_conn = Dbi.connect(host=host, user=user, passwd=passwd, db=db)
        self.debug = debug

    def close(self):
        self.db_conn.commit()

    def execute(self, sql, params=None):
        cursor = self.db_conn.cursor(cursorclass=Dbi.cursors.DictCursor)

        if self.debug:
            print sql, params

        if params:
            cursor.execute(sql, params)
        elif hasattr(sql, 'uninterpolated_sql') and sql.params:
            cursor.execute(sql.uninterpolated_sql(), sql.params)
        else:
            cursor.execute(str(sql))
        #for row in cursor.fetchall():
        #	yield row
        out = cursor.fetchall()
        cursor.close()
        return out

    def last_insert_id(self):
        return self.db_conn.insert_id()

class Query(object):
    def __init__(self):
        self.columns = []
        self.tables = []
        self.where = []
        self.group_by = []
        self.order_by = []
        self.limit = None
        self.offset = 0
        self.params = {}

    def uninterpolated_sql(self):
        sql = "SELECT " + ",\n\t\t".join(self.columns)
        # FIXME: flexible left/straight join
        sql += "\n\tFROM " + "\n\t\tLEFT JOIN ".join(self.tables)
        if self.where:
            sql += "\n\tWHERE " + "\n\t\tAND ".join(self.where)
        if self.group_by:
            sql += "\n\tGROUP BY " + ", ".join(self.group_by)
        if self.order_by:
            sql += "\n\tORDER BY " + ", ".join(self.order_by)
        if self.limit:
            sql += "\n\tLIMIT %d OFFSET %d" % (self.limit, self.offset)
        return sql

    def __repr__(self):
        # FIXME:
        qparams = {}
        for k, s in self.params.items():
            qparams[k] = "'%s'" % s
        return self.uninterpolated_sql() % qparams


db_conn = dict()

def get_db(schema=None):
    '''Convenience'''
    global db_conn

    if not schema:
        schema = config.db_params.db

    if schema not in db_conn:
        params = config.db_params
        params['db'] = schema
        db_conn[schema] = Connection(**params)

    return db_conn[schema]

def close_all():
    for conn in db_conn.values():
        conn.close()

atexit.register(close_all)
