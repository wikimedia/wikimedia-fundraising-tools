'''
Parse test descriptions

centralnotice import is hidden in a lazy hack until presentation driver is decoupled
'''

import time_util
import centralnotice
import contributions
import re

def read_gdoc_spec(doc=None):
    import gdocs
    return list(parse_spec(gdocs.Spreadsheet(doc=doc).get_all_rows()))

def parse_spec(spec):
    for row in spec:
        if 'disabled' in row and hasattr(row['disabled'], 'strip') and row['disabled'].strip():
            #print "DEBUG: Skipping disabled test spec: %s" % str(row)
            yield False #FIXME - this is a stupid placeholder
            continue
        yield FrTest(**row)

def update_gdoc_spec(doc=None, old_spec=None):
    '''
    Try to reconstruct actual tests by parsing centralnotice change logs
    '''
    import gdocs

    if not old_spec:
        old_spec = read_gdoc_spec(doc=doc)

    print "Updating test specs with latest CentralNotice changes... ", doc
    spec_doc = gdocs.Spreadsheet(doc=doc)

    def is_relevant(entry):
        '''
        This change enabled/disabled a test; or an enabled test has been mutated.
        '''
        if 'enabled' in entry['added'] or entry['begin']['enabled'] is 1:
            return True

    def test_from_entry(entry, edge):
        # FIXME: assuming this is a banner test
        banners = entry[edge]['banners']
        if hasattr(banners, 'keys'):
            banners = banners.keys()

        test = FrTest(
            type="banner",
            campaigns=entry['campaign'],
            banners=banners,
            start=entry[edge]['start'],
            end=entry[edge]['end'],
        )

        # FIXME x 2
        test.timestamp = entry['timestamp']
        if edge == 'begin':
            test.end_time = entry['timestamp']
        else:
            test.start_time = entry['timestamp']

        return test

    def find_test(spec, test):
        '''
        I'd like to do start_time match to discriminate between mutations of an otherwise identical test...
        Maybe this isn't DTRT, but something similar needs to happen.
        '''
        #print "DEBUG: Searching for ", test, " in ", spec
        # FIXME hardcoding missing "end" logic.
        for index, existing in enumerate(spec):
            if not existing:
                continue
            if test.campaigns == existing.campaigns and test.banners == existing.banners:
                return index

    logs = centralnotice.get_campaign_logs(since=time_util.str_time_offset(days=-1))
    changes = [ e for e in logs if is_relevant(e) ]
    changes.reverse()

    spec = old_spec
    for entry in changes:
        if 'enabled' in entry['removed'] and entry['removed']['enabled']:
            test_ending = test_from_entry(entry, 'begin')
            match = find_test(spec, test_ending)
            if match is not None:
                spec[match] = test_ending
                print "DEBUG: updating end time in row", match, test_ending
                spec_doc.update_row({'end': test_ending.timestamp}, index=match+1)
            else:
                print "DEBUG: not altering out-of-scope test: ", test_ending
        else:
            print "DEBUG: not mutating test at ", entry['timestamp']

        if 'enabled' in entry['added'] and entry['added']['enabled']:
            test_beginning = test_from_entry(entry, 'end')
            match = find_test(spec, test_beginning)
            if match is None:
                if test_beginning.label:
                    #FIXME hack:
                    spec.append(test_beginning)
                    # XXX test_beginning.__dict__ + 
                    props = {
                        'label': test_beginning.label,
                        'type': "banner",
                        'start': test_beginning.timestamp,
                        'end': test_beginning.end_time,
                        'campaigns': ", ".join([ c['name'] for c in test_beginning.campaigns ]),
                        'banners': ", ".join(test_beginning.banners),
                    }
                    spec_doc.append_row(props)
                #else:
                #    props['disabled'] = "X"
            else:
                spec[match].timestamp = test_beginning.timestamp
                spec_doc.update_row({'end': test_beginning.end_time}, index=match+1)
                print "DEBUG: re-opening existing test by updating end time: row ", match, test_beginning
        else:
            print "DEBUG: not mutating test at ", entry['timestamp']

    return spec

def write_gdoc_results(doc=None, results=[]):
    import gdocs
    print "Writing test results to %s" % doc
    doc = gdocs.Spreadsheet(doc=doc)
    for result in results:
        props = {}
        props.update(result['criteria'])
        props.update(result['results'])
        doc.append_row(props)

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
