'''
Mysql wrapper which allows query composition
'''
import MySQLdb as Dbi

class Connection(object):
    def __init__(self, host=None, user=None, passwd=None, db=None, debug=False):
        self.db_conn = Dbi.connect(host=host, user=user, passwd=passwd, db=db)
        self.cursor = self.db_conn.cursor(cursorclass=Dbi.cursors.DictCursor)
        self.debug = debug

    def close(self):
        #self.db_conn.commit()
        pass

    def execute(self, sql):
        if self.debug:
            print sql
        self.cursor.execute(str(sql))
        for row in self.cursor.fetchall():
            yield row

class Query(object):
    def __init__(self):
        self.columns = []
        self.tables = []
        self.where = []
        self.order_by = []
        self.group_by = []

    def __repr__(self):
        sql = "SELECT " + ",\n\t\t".join(self.columns)
        sql += "\n\tFROM " + "\n\t\t".join(self.tables)
        if self.where:
            sql += "\n\tWHERE " + "\n\t\tAND ".join(self.where)
        if self.order_by:
            sql += "\n\tORDER BY " + ", ".join(self.order_by)
        if self.group_by:
            sql += "\n\tGROUP BY " + ", ".join(self.group_by)
        return sql
