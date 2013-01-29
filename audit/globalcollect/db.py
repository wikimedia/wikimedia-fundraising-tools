import MySQLdb as Dbi

class Connection(object):
    def __init__(self, host=None, user=None, passwd=None, db=None, debug=False, **ignore):
        self.db_conn = Dbi.connect(host=host, user=user, passwd=passwd, db=db)
        self.debug = debug

    def close(self):
        self.db_conn.commit()
        pass

    def execute(self, sql, params=()):
        cursor = self.db_conn.cursor(cursorclass=Dbi.cursors.DictCursor)
        if self.debug:
            print(sql % params)
        cursor.execute(sql, params)
        cursor.close()
