from google.gdocs import Spreadsheet
from process.logging import Logger as log

def write_gdoc_results(doc=None, results=[]):
    log.info("Writing test results to {url}".format(url=doc))
    doc = Spreadsheet(doc=doc)
    for result in results:
        props = {}
        props.update(result['criteria'])
        props.update(result['results'])
        doc.append_row(props)

def update_gdoc_results(doc=None, results=[]):
    log.info("Updating results in {url}".format(url=doc))
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
                log.warn("more than one result row {match} matches criteria: {criteria}".format(match=matching, criteria=result['criteria']))
            index = matching[-1]
            log.debug("updating row {rownum} with {banner}".format(rownum=index, banner=result['criteria']['banner']))
            doc.update_row(props, index=index)
