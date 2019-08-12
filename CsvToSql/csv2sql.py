#!/usr/bin/python2

from optparse import OptionParser
import MySQLdb as MySQL
import csv
import re
import getpass
from dateutil.parser import parse as dateparse
from time import time

colproto = {
    'c': (lambda x: x, 'varchar(255)'),
    'i': (lambda x: int(x), 'integer'),
    'u': (lambda x: int(x), 'integer unsigned'),
    's': (lambda x: x, 'text'),
    'd': (lambda x: dateparse(x), 'datetime'),
    'f': (lambda x: float(x), 'float')
}


def main(host, port, user, password, database, tablename, delete, autoindex, schema, filename):
    global colproto

    # === Connection to MySQL ===
    db = MySQL.connect(host=host, port=port, user=user, passwd=password, db=database)

    # === Open the file ===
    f = open(filename, 'r')
    c = csv.reader(f)

    headers = c.next()

    # === Create the schema ===
    if len(headers) != len(schema):
        raise Exception('Num of columns must be the same as num of schema cols')
    if re.match("[^%s]" % ''.join(colproto.keys()), schema):
        raise Exception("Unknown schema column type detected")

    schemaStr = []
    insertCols = []
    schemaCopy = schema
    if autoindex:
        schemaStr.append("id int auto_increment primary key")
    for header in headers:
        # Normalize the Name
        header = re.sub('(^[0-9])|[^0-9a-zA-Z]', '', header)
        schemaStr.append("%s %s" % (header, colproto[schemaCopy[0]][1]))
        insertCols.append(header)
        schemaCopy = schemaCopy[1:]

    schemaStr = ', '.join(schemaStr)
    insertCols = ', '.join(insertCols)
    queryStr = "CREATE TABLE %s (%s);" % (tablename, schemaStr)
    print("Executing: %s" % queryStr)

    cur = db.cursor()
    if delete:
        cur.execute("DROP TABLE IF EXISTS %s;" % tablename)
    cur.execute(queryStr)

    # Now inject all the rows!
    start = time()
    count = 0
    queryStr = "INSERT INTO %s (%s) VALUES (%s);" % (tablename, insertCols, (','.join(['%s'] * len(headers))))
    for row in c:
        rowVals = []
        for i in range(0, len(row)):
            ctype = schema[i]
            func = colproto[ctype][0]
            rowVals.append(func(row[i]))

        cur.execute(queryStr, rowVals)
        count += 1
        if count % 2500 == 0:
            db.commit()
            print("%s - %s qps" % (count, 2500 / (time() - start)))
            start = time()
    print(count)
    db.commit()

    cur.close()
    db.close()
    f.close()


if __name__ == "__main__":
    user = getpass.getuser()

    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] schema file.csv")
    parser.add_option("-s", "--host", dest='host', default='localhost', help='MySQL server name [:port]')
    parser.add_option("-d", "--database", dest='database', default=user, help='MySQL database name')
    parser.add_option("-u", "--user", dest='user', default=user, help='MySQL username')
    parser.add_option("-p", "--password", dest='password', default=None, help='MySQL password, if none will prompt')
    parser.add_option("-t", "--table", dest='tablename', default=None, help='Name of table to create, otherwise will be sanitized file name')
    parser.add_option("-x", "--explain", dest='explainSchema', default=False, action='store_true', help="Describe schema options")
    parser.add_option("--delete", dest='delete', default=False, action='store_true', help="Delete table if exists")
    parser.add_option("-i", "--autoindex", dest='autoindex', default=False, action='store_true', help="Add auto increment pri key")
    (options, args) = parser.parse_args()

    if options.explainSchema:
        print("A schema is a simple string of characters from the following set that expands into SQL types:")
        for t in colproto:
            print("%s - %s" % (t, colproto[t][1]))
        exit()
    elif len(args) != 2:
        parser.print_usage()
        exit()

    if options.password is None:
        password = getpass.getpass("MySQL password for %s> " % options.user)
    else:
        password = options.password

    if options.tablename is None:
        tablename = re.sub('(^[0-9])|[^0-9a-zA-Z]', '_', args[1])
    else:
        tablename = options.tablename

    serverHP = options.host.split(':')
    hostname = serverHP[0]
    if len(serverHP) > 1:
        hostport = int(serverHP[1])
    else:
        hostport = 3306

    # === Launch the application ===
    main(
        host=hostname,
        port=hostport,
        user=options.user,
        password=password,
        database=options.database,
        tablename=tablename,
        delete=options.delete,
        autoindex=options.autoindex,
        schema=args[0],
        filename=args[1]
    )
