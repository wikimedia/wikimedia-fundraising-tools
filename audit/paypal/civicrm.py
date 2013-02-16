import MySQLdb as Dbi

db_conn = False

class Civicrm(object):
    def __init__(self, config):
        self.db = Connection(**dict(config.items("Db")))
        self.config = config

    def transaction_exists(self, gateway_txn_id, gateway="paypal"):
        sql = """
SELECT COUNT(*) AS count FROM wmf_contribution_extra
    WHERE gateway = %s AND gateway_txn_id = %s
        """

        count = list(self.db.execute(sql, (gateway, gateway_txn_id)))
        return count[0]['count'] > 0

class Connection(object):
    def __init__(self, host=None, user=None, passwd=None, db=None, debug=False):
        self.db_conn = Dbi.connect(host=host, user=user, passwd=passwd, db=db)
        self.debug = debug.lower() in ("true", "1")

    def close(self):
        #self.db_conn.commit()
        pass

    def execute(self, sql, params=None):
        cursor = self.db_conn.cursor(cursorclass=Dbi.cursors.DictCursor)
        if self.debug:
            print sql, params
        cursor.execute(str(sql), params)
        for row in cursor.fetchall():
            yield row
        cursor.close()
