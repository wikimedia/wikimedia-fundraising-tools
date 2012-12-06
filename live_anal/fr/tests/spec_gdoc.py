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
    last_row = doc.num_rows()
    for test in spec.spec:
        if test.source_index <= last_row:
            doc.update_row({'end': test.end_time}, index=test.source_index)
        else:
            doc.append_row({
                'label': test.label,
                'type': "banner",
                'start': test.timestamp,
                'end': test.end_time,
                'campaigns': ", ".join([ c['name'] for c in test.campaigns ]),
                'banners': ", ".join(test.banners),
            })

def write_gdoc_results(doc=None, results=[]):
    print "Writing test results to %s" % doc
    doc = Spreadsheet(doc=doc)
    for result in results:
        props = {}
        props.update(result['criteria'])
        props.update(result['results'])
        doc.append_row(props)

def update_gdoc_results(spec, results=[]):
    print "Updating results in %s" % doc
    doc = gdocs.Spreadsheet(doc=doc)
    existing = list(doc.get_all_rows())

    def find_matching_cases(criteria):
        matching = []

        def compare_row(row, criteria):
            if not row:
                return False
            for k, v in result['criteria'].items():
                if row[k] != v:
                    return False
            return True

        for n, row in enumerate(existing, 1):
            if compare_row(row, criteria):
                matching.append(n)

        return matching

    for result in results:
        if not result:
            continue

        matching = find_matching_cases(result['criteria'])

        if len(matching) == 0:
            props = {}
            props.update(result['criteria'])
            props.update(result['results'])
            doc.append_row(props)
        else:
            if len(matching) > 1:
                print "WARNING: more than one result row %s matches criteria: %s" % (matching, result['criteria'], )
            index = matching[-1]
            print "DEBUG: updating row %d" % index
            doc.update_row(result['results'], index=index)
