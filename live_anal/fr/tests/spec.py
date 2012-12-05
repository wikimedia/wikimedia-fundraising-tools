'''
TODO:
* match start_time to discriminate between mutations of an otherwise identical test...
'''

import re

import time_util
import campaign_log

try:
    #import jail
    import gdocs

    def read_gdoc_spec(doc=None):
        return list(parse_spec(gdocs.Spreadsheet(doc=doc).get_all_rows()))

    def update_gdoc_spec(doc=None, old_spec=None):
        '''
        Try to reconstruct actual tests by parsing centralnotice change logs
        '''

        print "Updating test specs with latest CentralNotice changes... ", doc

        if not old_spec:
            old_spec = read_gdoc_spec(doc=doc)

    def write_gdoc_results(doc=None, results=[]):
        print "Writing test results to %s" % doc
        doc = gdocs.Spreadsheet(doc=doc)
        for result in results:
            props = {}
            props.update(result['criteria'])
            props.update(result['results'])
            doc.append_row(props)


    class GdocSpecfile(Specfile):
        def __init__(self, doc=None):
            super(Specfile, self).__init__()

            self.spec = read_gdoc_spec(doc=doc)

            self.doc = gdocs.Spreadsheet(doc=doc)

        def append_test(self, test):
            super(Specfile, self).append_test(test)

            self.doc.append_row(test)

except ImportError:
    pass


def parse_spec(spec):
    for row in spec:
        if 'disabled' in row and hasattr(row['disabled'], 'strip') and row['disabled'].strip():
            #print "DEBUG: Skipping disabled test spec: %s" % str(row)
            yield False #FIXME - this is a stupid placeholder
            continue
        yield FrTest(**row)


def compare_test_fuzzy(a, b):
    if a.campaigns == b.campaigns and a.banners == b.banners:
        return True

class Specfile(object):
    '''or other storage format'''
    def __init__(self):
        self.spec = []

    def append_test(self, test):
        self.spec.append(test)

    def update_test(self, test, update=None, index=None, insert=False):
        if not update:
            update = test
        if not index:
            index = self.find_test(test)
        if index:
            self.spec[match] = test_ending
        elif insert:
            sepc.spec.append(test_ending)

    def find_test(test):
        for index, existing in enumerate(self.spec):
            if existing and compare_test_fuzzy(test, existing):
                return index


def update_spec(spec):
    for test_ending, test_beginning in campaign_log.get_relevant_events():
        if test_ending:
            spec.update_test(test_ending)

        if test_beginning:
            #XXX
            if test_beginning.label:
                spec.update_test(test_beginning)

    return spec

def update_gdoc_results(doc=None, results=[]):
    import gdocs
    print "Updating results in %s" % doc
    doc = gdocs.Spreadsheet(doc=doc)
    existing = list(doc.get_all_rows())

    def find_rows(criteria):
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

        matching = find_rows(result['criteria'])

        if len(matching) == 0:
            props = {}
            props.update(result['criteria'])
            props.update(result['results'])
            doc.append_row(props)
        else:
            if len(matching) > 1:
                print "Warning: more than one result row %s matches criteria: %s" % (matching, result['criteria'], )
            index = matching[-1]
            print "DEBUG: updating row %d" % index
            doc.update_row(result['results'], index=index)

class FrTest(object):
    '''
    '''
    def __init__(self, label=None, type="", campaigns=None, banners=None, start=None, end=None, **ignore):
        import centralnotice

        #print "Warning: ignoring columns: %s" % (", ".join(ignore.keys()), )

        self.campaigns = []
        if campaigns:
            if hasattr(campaigns, 'split'):
                campaigns = [ s.strip() for s in campaigns.split(",") ]
            for name in campaigns:
                campaign = centralnotice.get_campaign(name)
                if campaign:
                    campaign['name'] = name
                    self.campaigns.append(campaign)
                else:
                    print "Warning: no such campaign '%s'" % name

        type = type.lower()

        if type.count('banner') > 0:
            self.is_banner_test = True
            if banners:
                if hasattr(banners, 'strip'):
                    banners = [ s.strip() for s in banners.split(",") ]
                self.banners = banners
            else:
                def reduce_banners(sum, campaign):
                    if campaign['banners']:
                        sum.extend(campaign['banners'].keys())
                    return sum

                self.banners = reduce(reduce_banners, self.campaigns, [])

        self.is_country_test = (type.count('country') > 0)
        self.is_lp_test = (type.count('lp') > 0)

        self.type = type

        self.start_time = start
        self.end_time = end

        if label:
            self.label = label
        else:
            self.label = ""
            if self.banners:
                match = re.search(r"_\d+_([^_]+)_", self.banners[0])
                if match and match.group(1):
                    self.label = match.group(1)

        self.results = []

    def load_results(self):
        for campaign in self.campaigns:
            start = self.start_time
            if not start:
                start = campaign['start']
            end = self.end_time
            if not end:
                end = campaign['end']

            campaign_condition = {
                'campaign': campaign['name'],
                'start': start,
                'end': end,
            }
            def calculate_result(**criteria):
                cond = {}
                cond.update(campaign_condition)
                cond.update(criteria)
                result = contributions.get_totals(**cond)
                return TestResult(result, **cond)

            if self.is_banner_test and self.banners:
                results = []
                for name in self.banners:
                    result = calculate_result(banner=name)
                    result.results['preview'] = "http://en.wikipedia.org/wiki/Special:Random?banner=" + name
                    result.results['label'] = self.label
                    results.append(result)
                try:
                    confidence = self.get_confidence(results, 'banner', 'donations')
                    for i, levels in enumerate(confidence):
                        results[i].results['p-value'] = levels.two_tailed_p_value
                except ImportError as e:
                    print "ERROR: not calculating confidence, dummy: ", e.strerror
                results[0].results['confidence_link'] = self.get_confidence_link(results, 'banner', 'donations', 100000)
                self.results.extend(results)

            if self.is_country_test:
                results = [ calculate_result(country=code) for code in campaign['countries'] ]
                self.results.extend(results)

        if self.is_lp_test:
            print "LP test type not implemented"
            pass

    def __repr__(self):
        description = '''
Test: %(type)s (%(campaigns)s) %(start)s - %(end)s
''' % {'type': self.type, 'campaigns': str([c['name'] for c in self.campaigns]), 'start': self.start_time, 'end': self.end_time, }
        if self.is_banner_test:
            description += " banners: " + str(self.banners)
        if self.is_country_test:
            description += " countries: " + str(self.countries)
        if self.is_lp_test:
            description += " lps: " + str(self.lps)
        return description

    def get_confidence(self, results, name_column=None, successes_column=None, trials=None):
        from stats_abba import Experiment
        FUDGE_TRIALS = 100000
        CONFIDENCE_LEVEL=0.95
        num_test_cases = len(results)

        if not num_test_cases:
            return
        baseline_successes = results[0].results[successes_column]
        if not baseline_successes:
            #FIXME
            return

        print "DEBUG: ", results
        experiment = Experiment(
            num_trials=FUDGE_TRIALS,
            baseline_num_successes=baseline_successes,
            baseline_num_trials=FUDGE_TRIALS,
            confidence_level=CONFIDENCE_LEVEL
        )
        #useMultipleTestCorrection=true

        cases = []
        for result in results:
            name = result.results[name_column]
            successes = result.results[successes_column]
            if hasattr(trials, 'encode'):
                trials = result.results[trials]
            else:
                trials = FUDGE_TRIALS
            cases.append(experiment.get_results(num_successes=successes, num_trials=trials))

        return cases

    def get_confidence_link(self, results, name_column, successes_column, trials):
        cases = []
        for result in results:
            name = result.results[name_column]
            successes = result.results[successes_column]
            if hasattr(trials, 'encode'):
                trials = result.results[trials]
            cases.append( "%s=%s,%s" % (name, successes, trials) )
        return "http://www.thumbtack.com/labs/abba/#%s&abba:intervalConfidenceLevel=0.95&abba:useMultipleTestCorrection=true" % "&".join(cases)

class TestResult(object):
    def __init__(self, results, **criteria):
        self.criteria = criteria
        self.results = results

    def __repr__(self):
        import json
        return '''
Result: %s
  -> %s''' % (json.dumps(self.criteria, indent=4), json.dumps(self.results, indent=4), )

class TestSpecException(Exception):
    def __init__(self, msg):
        super(Exception, self).__init__("Bad test specifier: " + msg) 
