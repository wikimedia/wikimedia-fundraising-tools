from spec import FrTestSpec, parse_spec
from google.gdocs import Spreadsheet
from process.logging import Logger as log

def read_gdoc_spec(doc=None):
    return FrTestSpec(spec=list(parse_spec(Spreadsheet(doc=doc).get_all_rows())))

def update_gdoc_spec(doc=None, spec=None):
    log.info("Updating test specs with latest CentralNotice changes... {url}".format(url=doc))

    # FIXME: currently, the spec must have been read with read_gdoc_spec in order to get row numbers
    if not spec:
        spec = read_gdoc_spec(doc=doc)

    spec.update_from_logs()

    doc = Spreadsheet(doc=doc)
    for index, test in enumerate(spec.spec, 0):
        test = spec.spec[index]
        rownum = index + 1
        if rownum < doc.num_rows():
            if not hasattr(test, 'modified') or not test.modified:
                continue
            log.debug("updating spec end time in row {rownum}: {spec}".format(rownum=rownum, spec=test))
            if test.end_time:
                doc.update_row({'end': test.end_time}, index=rownum)
        else:
            log.debug("appending spec row {rownum}: {spec}".format(rownum=index, spec=test))
            doc.append_row({
                'label': test.label,
                'type': "banner",
                'start': test.start_time,
                'end': test.end_time,
                'campaign': test.campaign['name'],
                'banners': ", ".join(test.banners),
            })
