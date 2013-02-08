from gdocs import Spreadsheet

def write_gdoc_results(doc=None, results=[]):
    print "Writing test results to %s" % doc
    doc = Spreadsheet(doc=doc)
    for result in results:
        props = {}
        props.update(result['criteria'])
        props.update(result['results'])
        doc.append_row(props)

def update_gdoc_results(doc=None, results=[]):
    print "Updating results in %s" % doc
    doc = Spreadsheet(doc=doc)
    existing = list(doc.get_all_rows())

    def find_matching_cases(criteria):
        matching = []

        def fuzzy_compare_row(row, criteria):
            if not row:
                return False
            if criteria['banner'] == row['banner'] and criteria['campaign'] == row['campaign'] and criteria['start'] == row['start']:
                return True

        for n, row in enumerate(existing, 1):
            if fuzzy_compare_row(row, criteria):
                matching.append(n)

        return matching

    for result in results:
        if not result:
            continue

        matching = find_matching_cases(result['criteria'])

        props = {}
        props.update(result['results'])
        props.update(result['criteria'])

        if len(matching) == 0:
            doc.append_row(props)
        else:
            if len(matching) > 1:
                print "WARNING: more than one result row %s matches criteria: %s" % (matching, result['criteria'], )
            index = matching[-1]
            print "DEBUG: updating row %d with %s" % (index, result['criteria']['banner'])
            doc.update_row(props, index=index)
