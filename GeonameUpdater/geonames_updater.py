#!/usr/bin/python3.5

from configparser import SafeConfigParser
from optparse import OptionParser
import dateutil.parser
# import pytz
import pymysql as MySQL
import sys
import os
import re
import codecs

_config = None
_geonamesDB = None


def install_schema():
    global _geonamesDB
    cur = _geonamesDB.cursor()

    cur.execute("DROP TABLE IF EXISTS geonames;")
    cur.execute("""
        CREATE TABLE geonames (
          geonameid INT UNSIGNED NOT NULL PRIMARY KEY,
          name VARCHAR(200),
          ascii_name VARCHAR(200),
          latitude DOUBLE,
          longitude DOUBLE,
          feature_type_id INT UNSIGNED,
          country_code CHAR(2),
          admin1 VARCHAR(20),
          admin2 VARCHAR(80),
          admin3 VARCHAR(20),
          admin4 VARCHAR(20),
          population BIGINT,
          tzid INT UNSIGNED,
          last_modification DATE,

          INDEX idx_name (geonameid),
          INDEX idx_ascii_name (ascii_name),
          INDEX idx_lat (latitude),
          INDEX idx_long (longitude),
          INDEX idx_feature_type (feature_type_id),
          INDEX idx_country_code (country_code),
          INDEX idx_admin1 (country_code, admin1),
          INDEX idx_admin12 (country_code, admin1, admin2)
        ) ENGINE InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci;
    """)

    cur.execute("DROP TABLE IF EXISTS altnames;")
    cur.execute("""
        CREATE TABLE altnames (
          int_altnameid INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          altnameid INT UNSIGNED,
          geonameid INT UNSIGNED NOT NULL,
          format VARCHAR(8),
          altname VARCHAR(200),
          is_preferred TINYINT(1) DEFAULT 0,
          is_short TINYINT(1) DEFAULT 0,
          is_colloquial TINYINT(1) DEFAULT 0,
          is_historic TINYINT(1) DEFAULT 0,

          INDEX idx_altnameid (altnameid),
          INDEX idx_geonameid (geonameid),
          INDEX idx_altname (altname),
          INDEX idx_format (format)
        ) ENGINE InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci;
    """)

    cur.execute("DROP TABLE IF EXISTS timezones;")
    cur.execute("""
        CREATE TABLE timezones (
          tzid INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          country_code CHAR(2),
          tzname VARCHAR(40),
          offset FLOAT,

          INDEX idx_cc (country_code),
          INDEX idx_timezone (tzname),
          INDEX idx_offset (offset)
        ) ENGINE InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci;
    """)

    cur.execute("DROP TABLE IF EXISTS feature_types;")
    cur.execute("""
        CREATE TABLE feature_types (
          ftid INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          class CHAR(1),
          code VARCHAR(20),
          name VARCHAR(100),
          description VARCHAR(500),

          INDEX idx_feature_class (class),
          INDEX idx_feature_code (code),
          INDEX idx_feature_composite (class, code)
        ) ENGINE InnoDB CHARACTER SET utf8 COLLATE utf8_unicode_ci;
    """)

    cur.close()
    write("Done!\n")


def import_timezone_file(filename):
    global _geonamesDB

    # === Prepare things ===
    try:
        f = open(filename, 'r')
    except (IOError):
        writeErr("Could not open %s for read!" % filename)
        return

    # --- Clear the table ---
    write("Truncating table... ")
    cur = _geonamesDB.cursor()
    cur.execute("TRUNCATE timezones")

    # === Import data ===
    write("Importing... ")
    count = 0

    lre = re.compile(r"""^
            (?P<countryCode>[^\t]*)\t
            (?P<tzName>[^\t]*)\t
            (?P<gmtOffset>[^\t]*)\t
            (?P<dstOffset>[^\t]*)\t
            (?P<utcOffset>[^\t]*)$
        """, re.VERBOSE)

    line = f.readline().strip()    # The first line is a header
    if line != "CountryCode	TimeZoneId	GMT offset 1. Jan 2013	DST offset 1. Jul 2013	rawOffset (independant of DST)":
        writeErr("File %s does not have the correct header!" % filename)
    else:
        for line in f:
            line = line[:-1]
            m = lre.match(line)
            if not m:
                writeErr("Line '%s' could not be parsed!" % line)
                continue

            cc = m.group('countryCode')
            name = m.group('tzName')
            offset = float(m.group('utcOffset'))

            cur.execute("INSERT INTO timezones (country_code, tzname, offset) VALUES (%s, %s, %s);", (cc, name, offset))
            count = count + 1

    cur.close()
    f.close()
    write("Done (%s records)!\n" % count)


def import_features_file(filename):
    global _geonamesDB

    # === Prepare things ===
    try:
        f = open(filename, 'r')
    except (IOError):
        writeErr("Could not open %s for read!" % filename)
        return

    # --- Clear the table ---
    write("Truncating table... ")
    cur = _geonamesDB.cursor()
    cur.execute("TRUNCATE feature_types")

    # === Import data ===
    write("Importing... ")
    count = 0

    lre = re.compile(r"""^
            (?P<class>[A-Z0-9])\.
            (?P<code>[A-Z0-9]*)\t
            (?P<name>[^\t]*)\t?
            (?P<desc>.*)
        $""", re.VERBOSE)

    for line in f:
        line = line[:-1]
        m = lre.match(line)
        if not m:
            writeErr("Line '%s' could not be parsed!" % line)
            continue

        cur.execute(
            "INSERT INTO feature_types (class, code, name, description) VALUES (%s, %s, %s, %s);",
            (m.group('class'), m.group('code'), m.group('name'), m.group('desc'))
        )
        count = count + 1

    cur.close()
    f.close()
    write("Done (%s records)!\n" % count)


def import_geonames_file(filename):
    global _geonamesDB

    # === Prepare things ===
    try:
        f = codecs.open(filename, 'r', 'utf-8')
    except (IOError):
        writeErr("Could not open %s for read!" % filename)
        return

    # --- Clear the table ---
    write("Truncating table... ")
    cur = _geonamesDB.cursor()
    cur.execute("TRUNCATE geonames;")

    # === Import data ===
    write("Importing ")
    count = 0

    lre = re.compile(r"""^
            (?P<geonameid>[^\t]*)\t
            (?P<name>[^\t]*)\t
            (?P<asciiname>[^\t]*)\t
            (?P<altnames>[^\t]*)\t
            (?P<lat>[^\t]*)\t
            (?P<long>[^\t]*)\t
            (?P<featureClass>[^\t]*)\t
            (?P<featureCode>[^\t]*)\t
            (?P<countryCode>[^\t]*)\t
            (?P<cc2>[^\t]*)\t
            (?P<admin1>[^\t]*)\t
            (?P<admin2>[^\t]*)\t
            (?P<admin3>[^\t]*)\t
            (?P<admin4>[^\t]*)\t
            (?P<pop>[^\t]*)\t
            (?P<elevation>[^\t]*)\t
            (?P<dem>[^\t]*)\t
            (?P<tz>[^\t]*)\t
            (?P<date>[^\t]*)
        $""", re.VERBOSE)

    for line in f:
        line = line[:-1]
        m = lre.match(line)
        if not m:
            writeErr("Line '%s' could not be parsed!" % line)
            continue

        cur.execute("""
                INSERT INTO geonames (
                    geonameid, name, ascii_name, latitude, longitude, feature_type_id, country_code,
                    admin1, admin2, admin3, admin4, population, tzid, last_modification
                )
                SELECT %s, %s, %s, %s, %s, ftid, %s, %s, %s, %s, %s, %s, tzid, %s
                FROM timezones, feature_types ft WHERE tzname=%s AND ft.class=%s AND ft.code=%s;
            """,
            (  # noqa
                int(m.group('geonameid')),
                m.group('name'),
                m.group('asciiname'),
                float(m.group('lat')),
                float(m.group('long')),
                m.group('countryCode'),
                m.group('admin1'),
                m.group('admin2'),
                m.group('admin3'),
                m.group('admin4'),
                int(m.group('pop')),
                dateutil.parser.parse(m.group('date')),
                m.group('tz'),
                m.group('featureClass'),
                m.group('featureCode'),
            )
        )
        altnames = m.group('altnames').split(',')
        if altnames:
            id = int(m.group('geonameid'))
            altnamereplace = []
            for an in altnames:
                altnamereplace.append((id, an))

            cur.executemany(
                "INSERT INTO altnames (geonameid, format, altname) VALUES (%s, 'translit', %s);",
                altnamereplace
            )
        count = count + 1
        if count % 1000 == 0:
            write(".")

    cur.close()
    f.close()
    write("Done (%s records)!\n" % count)


def import_altnames_file(filename):
    global _geonamesDB

    # === Prepare things ===
    try:
        f = codecs.open(filename, 'r', 'utf-8')
    except (IOError):
        writeErr("Could not open %s for read!" % filename)
        return

    # --- Clear the table ---
    write("Truncating table... ")
    cur = _geonamesDB.cursor()
    cur.execute("TRUNCATE altnames;")

    # === Import data ===
    write("Importing ")
    count = 0

    for line in f:
        line = line[:-1]
        m = line.split("\t")
        if len(m) != 8:
            writeErr("Line '%s' could not be parsed! (%d parts)" % (line, len(m)))
            continue

        cur.execute("""
                INSERT INTO altnames (
                    altnameid, geonameid, format, altname, is_preferred, is_short, is_colloquial, is_historic
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (  # noqa
                int(m[0]),
                int(m[1]),
                m[2],
                m[3],
                int(m[4] or 0),
                int(m[5] or 0),
                int(m[6] or 0),
                int(m[7] or 0)
            )
        )
        count = count + 1
        if count % 1000 == 0:
            write(".")

    cur.close()
    f.close()
    write("Done (%s records)!\n" % count)


def write(str):
    sys.stdout.write(str.encode('utf-8'))
    sys.stdout.flush()


def writeErr(str):
    sys.stdout.write("%s\n" % str.encode('utf-8'))
    sys.stdout.flush()


if __name__ == "__main__":
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] <file root>")
    parser.add_option("-c", "--config", dest='configFile', default=None, help='Path to configuration file')
    parser.add_option('--install-db', dest='installDb', action='store_true', default=False, help='Install the schema')
    parser.add_option('--import-tz', dest='importTz', action='store_true', default=False, help='Import timeZones.txt (must happen before --import-gn)')
    parser.add_option('--import-f', dest='importF', action='store_true', default=False, help='Import featureCodes_en.txt (must happen before --import-gn)')
    parser.add_option('--import-gn', dest='importGn', action='store_true', default=False, help='Import allCountries.txt')
    parser.add_option('--import-alt', dest='importAlt', action='store_true', default=False, help='Import alternateNames.txt (must happen before --import-gn)')
    (options, args) = parser.parse_args()

    if len(args) == 0:
        filepath = os.getcwd()
    elif len(args) == 1:
        filepath = args[1]
    else:
        parser.print_usage()
        exit()

    # === Do some initial setup of useful globals ===
    # --- Like the configuration file :) ---
    localdir = os.path.dirname(os.path.abspath(__file__))
    _config = SafeConfigParser()
    fileList = ["%s/geonames.cfg" % localdir]
    if options.configFile is not None:
        fileList.append(options.configFile)
    _config.read(fileList)

    # --- And the MySQL connection ---
    _geonamesDB = MySQL.connect(
        _config.get('MySQL', 'host'),
        _config.get('MySQL', 'user'),
        _config.get('MySQL', 'password'),
        _config.get('MySQL', 'geonamesDB'),
        use_unicode=True,
        charset='utf8'
    )
    _geonamesDB.autocommit(True)

    # === Run the requested actions! ===
    if options.installDb:
        write("Installing Geonames schema... ")
        install_schema()

    if options.importTz:
        write("Importing TimeZone data... ")
        import_timezone_file("%s/timeZones.txt" % filepath)

    if options.importF:
        write("Importing feature types... ")
        import_features_file("%s/featureCodes_en.txt" % filepath)

    if options.importAlt:
        write("Importing Alternate Names data... ")
        import_altnames_file("%s/alternateNames.txt" % filepath)

    if options.importGn:
        write("Importing GeoNames data... ")
        import_geonames_file("%s/allCountries.txt" % filepath)

    # === Cleanup
    _geonamesDB.close()
