#import jail
from spec import FrTestSpec, parse_spec
from gdocs import Spreadsheet

def read_gdoc_spec(doc=None):
    return FrTestSpec(spec=list(parse_spec(Spreadsheet(doc=doc).get_all_rows())))

def update_gdoc_spec(doc=None, spec=None):
    print "Updating test specs with latest CentralNotice changes... ", doc

    # FIXME: currently, the spec must have been read with read_gdoc_spec in order to get row numbers
    if not spec:
        spec = read_gdoc_spec(doc=doc)

    spec.update_from_logs()

    doc = Spreadsheet(doc=doc)
    last_row = doc.num_rows() - 1
    for test in spec.spec:
        if not hasattr(test, 'modified') or not test.modified:
            continue
        rownum = int(test.source_index)+1
        if rownum <= last_row:
            print "DEBUG: updating spec row %d" % rownum
            print test
            if test.end_time:
                doc.update_row({'end': test.end_time}, index=rownum)
        else:
            print "DEBUG: appending spec row"
            doc.append_row({
                'label': test.label,
                'type': "banner",
                'start': test.start_time,
                'end': test.end_time,
                'campaigns': ", ".join([ c['name'] for c in test.campaigns ]),
                'banners': ", ".join(test.banners),
            })
