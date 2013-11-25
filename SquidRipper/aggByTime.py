#!/usr/bin/python

from optparse import OptionParser
import sys
from dateutil.parser import parse as dateParse
from datetime import datetime

def main():
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] <timeColumn> <timeInterval> <groupByColumn> ...")
    parser.add_option(
        '-p', '--pivot', dest='pivot', action='store_true', default=False,
        help='store the data until the end of the stream and then pivot it into groupByCol groups (SIGNIFICANT MEMORY USAGE)' # noqa
    )
    parser.add_option('-s', dest='sep', default='|', help='groupByCol separator when pivoting')
    parser.add_option('-m', '--multplier', dest='multiplier', default=100)
    (options, args) = parser.parse_args()

    if len(args) < 3:
        parser.print_usage()
        exit()

    pivot = options.pivot
    colNameSep = options.sep
    multiplier = int(options.multiplier)
    timeCol = int(args[0])
    interval = int(args[1])
    groupCols = []
    for i in range(2, len(args)):
        groupCols.append(int(args[i]))

    # Data is a complex data structure; with the following layout:
    # Timestamp (start of interval)
    # -- {(col1 val, col2 val, col3 val...)}
    # -- -- Count
    data = {}

    # Similarly, if we're pivoting we'll keep track of unique columns through time
    # (col1 val, col2 val, col3 val...)
    uniqueCols = set()

    lineCount = 0
    for line in sys.stdin:
        parts = line.strip().split(' ')

        # Find the agg time
        ctime = int(dateParse(parts[timeCol]).strftime('%s'))  # Yes, this is horribly inefficient; meh
        ctime = (ctime / interval) * interval

        colVals = []
        for i in groupCols:
            colVals.append(parts[i])
        colVals = tuple(colVals)

        if not ctime in data:
            data[ctime] = {}
        if not colVals in data[ctime]:
            data[ctime][colVals] = 1
        else:
            data[ctime][colVals] += 1

        if not pivot:
            lineCount = (lineCount + 1) % 1000
            if lineCount == 0:
                # Flush the buffers if possible
                for ptime in sorted(data.keys()):
                    if ptime + (2 * interval) < ctime:
                        for dataline in data[ptime]:
                            sys.stdout.write("%s\t%s\t%s\n" % (
                                datetime.fromtimestamp(ptime).strftime('%Y-%m-%d %H:%M:%S'),
                                colNameSep.join(dataline),
                                data[ptime][dataline] * multiplier
                            ))
                        del data[ptime]
        else:
            uniqueCols.add(colVals)

    # And here we are at the end...
    if pivot:
        # Must create the BIG table now
        outline = ['time']
        for cols in uniqueCols:
            outline.append(colNameSep.join(cols))
        sys.stdout.write("\t".join(outline))
        sys.stdout.write("\n")

        for ptime in sorted(data.keys()):
            outline = [datetime.fromtimestamp(ptime).strftime('%Y-%m-%d %H:%M:%S')]
            for cols in uniqueCols:
                if cols in data[ptime]:
                    outline.append(str(data[ptime][cols] * multiplier))
                else:
                    outline.append('0')
            sys.stdout.write("\t".join(outline))
            sys.stdout.write("\n")


if __name__ == "__main__":
    main()
