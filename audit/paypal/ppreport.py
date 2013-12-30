import io

from unicode_csv_reader import unicode_csv_reader

dialect = dict(
    delimiter=',',
    quotechar='"'
)

def read(path, version, callback):
    try:
        read_encoded(path, version, callback, encoding='utf-16')
    except UnicodeError:
        read_encoded(path, version, callback, encoding='utf-8-sig')

def read_encoded(path, version, callback, encoding):
    # Coerce to a list
    if not hasattr(version, 'extend'):
        version = [version]

    with io.open(path, 'r', encoding=encoding) as csvfile:
        plainreader = unicode_csv_reader(csvfile, **dialect)

        for row in plainreader:
            if row[0] == 'RH':
                if int(row[4]) not in version:
                    raise RuntimeError("This file uses an unexpected format revision: {version}".format(version=row[4]))
            elif row[0] == 'FH':
                pass
            elif row[0] == 'SH':
                start_date, end_date = row[1:2]
                log.info("Report file covers date range {start} to {end}".format(start=start_date, end=end_date))
            elif row[0] == 'CH':
                column_headers = ['Column Type'] + row[1:]
                break
            else:
                raise RuntimeError("Unexpected row type: {type}".format(type=row[0]))

        for line in plainreader:
            row = dict(zip(column_headers, line))
            if row['Column Type'] == 'SB':
                callback(row)
            elif row['Column Type'] in ('SF', 'SC', 'RF', 'RC', 'FF'):
                pass
            else:
                raise RuntimeError("Section ended and crazy stuff began: {type}".format(type=row['Column Type']))
