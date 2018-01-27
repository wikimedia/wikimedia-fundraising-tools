'''
Mysql wrapper providing query composition
'''
import MySQLdb as Dbi
import atexit
import os
import threading

from signal import signal, SIGTERM, SIG_DFL
from process.log import Logger as log
import process.globals


class Connection(object):
    def __init__(self, debug=False, **kw):
        self.connectionArgs = kw
        self.db_conn = Dbi.connect(**kw)
        self.debug = debug
        self.connection_id = self.execute('SELECT CONNECTION_ID() AS cid', None, 0)[0]['cid']

    def close(self):
        self.db_conn.commit()

    def execute(self, sql, params=None, timeout=0):
        cursor = self.db_conn.cursor(cursorclass=Dbi.cursors.DictCursor)
        deathClock = None

        if self.debug:
            if params:
                log.debug(str(sql) + " % " + repr(params))
            else:
                log.debug(str(sql))

        if timeout > 0:
            deathClock = threading.Timer(timeout, self.kill_connection)
            deathClock.start()

        try:
            if params:
                cursor.execute(sql, params)
            elif hasattr(sql, 'uninterpolated_sql') and sql.params:
                cursor.execute(sql.uninterpolated_sql(), sql.params)
            else:
                cursor.execute(str(sql))
            # for row in cursor.fetchall():
            #     yield row
            out = cursor.fetchall()
            cursor.close()
            return out
        finally:
            if deathClock is not None:
                deathClock.cancel()

    def kill_connection(self):
        log.warn('Query taking too long - killing connection {}'.format(self.connection_id))
        killerConnection = Dbi.connect(**self.connectionArgs)
        cursor = killerConnection.cursor()
        cursor.execute('KILL CONNECTION {}'.format(self.connection_id))
        killerConnection.close()

    def execute_paged(self, query, pageIndex, pageSize=1000, dir='ASC'):
        """ Execute a paged query. This will yield a dictionary of the results
        until there are no more results to yield. The pageIndex will be added
        to the order by automatically. If the Query already has a limit, it will
        be respected (only that many rows will be returned.)

        :param query: The Query object to run
        :param pageIndex: Name of the column to page over (should be numeric)
        :param pageSize: Number of rows to return per page
        :param dir: 'ASC' or 'DESC'; should the index be iterated in a positive or negative direction
        :return:
        """
        if not isinstance(query, Query):
            raise Exception('Paged query must start as a Query object')

        if query.limit:
            count = query.limit
        else:
            count = 0

        query.limit = pageSize
        query.order_by.append("%s %s" % (pageIndex, dir))

        lastId = None
        while True:
            results = self.execute(query)
            if len(results) == 0:
                break

            for result in results:
                yield result
                count -= 1
                if count == 0:
                    break

            if lastId is not None:
                del query.where[-1]
            lastId = result[pageIndex]
            query.params['lastId'] = lastId
            if dir == 'ASC':
                query.where.append("%s > %%(lastId)s" % (pageIndex))
            else:
                query.where.append("%s < %%(lastId)s" % (pageIndex))

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

    config = process.globals.get_config()

    if not schema:
        schema = config.db_params.db

    if schema not in db_conn:
        params = config.db_params
        params['db'] = schema
        db_conn[schema] = Connection(**params)

    return db_conn[schema]


def load_queries(script_path):
    '''
    Helper to parse queries out of a SQL file.  Requires that each statement
    ends in /.*;\s*$/
    '''
    qbuf = []
    queries = []
    f = open(script_path, 'r')
    for line in f:
        line = line.rstrip()
        if line:
            qbuf.append(line)
            if line.endswith(';'):
                query = "\n".join(qbuf)
                queries.append(query)

                qbuf = []

    f.close()
    return queries


def run_script(script_path):
    '''
    Load and run a SQL file using the current database connection.
    '''

    queries = load_queries(script_path)
    for statement in queries:
        get_db().execute(statement)


def close_all():
    for conn in db_conn.values():
        conn.close()


def handle_sigterm(signum, stack_frame):
    close_all()
    signal(SIGTERM, SIG_DFL)
    os.kill(os.getpid(), signum)


atexit.register(close_all)
signal(SIGTERM, handle_sigterm)
