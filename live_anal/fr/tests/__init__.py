'''
Test specifications, cases, and results.

These are not unit tests ;) they are WMF Fundraising A/B tests.
'''

import re

from fr.centralnotice import get_campaign
from fr.contributions import get_totals

FR_LABEL_PATTERN = r'_\d+_([^_]+)_'
FUDGE_TRIALS = 100000
CONFIDENCE_LEVEL = 0.95

class FrTest(object):
    def __init__(self, label=None, type="", campaigns=None, banners=None, start=None, end=None, source_index=0, **ignore):
        #print "Warning: ignoring columns: %s" % (", ".join(ignore.keys()), )

        self.campaigns = []
        if campaigns:
            if hasattr(campaigns, 'split'):
                campaigns = [ s.strip() for s in campaigns.split(",") ]
            for name in campaigns:
                campaign = get_campaign(name)
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
                match = re.search(FR_LABEL_PATTERN, self.banners[0])
                if match and match.group(1):
                    self.label = match.group(1)

        # FIXME: egregious hack until gdocs are cached
        self.source_index = source_index

        self.results = []

    def load_results(self):
        for campaign in self.campaigns:
            if self.is_banner_test and self.banners:
                results = []
                for name in self.banners:
                    test_case = self.get_case(campaign=campaign['name'], banner=name)
                    totals = get_totals(**test_case)

                    result_extra = {
                        'preview': "http://en.wikipedia.org/wiki/Special:Random?banner=" + name,
                        'label': self.label,
                    }

                    results.append(TestResult(
                        criteria=test_case,
                        results=[totals, result_extra]
                    ))

                try:
                    confidence = self.get_confidence(results, 'banner', 'donations')
                    for i, levels in enumerate(confidence):
                        results[i].add_result('p-value', levels.two_tailed_p_value)
                        results[i].add_result('improvement', levels.relative_improvement.value * 100)
                except ImportError as e:
                    print "ERROR: not calculating confidence, dummy: ", e.strerror
                results[0].add_result('confidencelink', self.get_confidence_link(results, 'banner', 'donations', 100000))

                self.results.extend(results)

            if self.is_country_test:
                results = [ calculate_result(country=code) for code in campaign['countries'] ]
                self.results.extend(results)

        if self.is_lp_test:
            print "LP test type not implemented"
            pass

    def get_case(self, **kw):
        conditions = {
            'start': self.start_time,
            'end': self.end_time,
        }
        conditions.update(**kw)
        return conditions

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
        description += " row: %(source_index)s " % {'source_index': self.source_index}
        return description

    def get_confidence(self, results, name_column=None, successes_column=None, trials=None):
        from stats_abba import Experiment
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
    def __init__(self, criteria=None, results=None):
        self.criteria = criteria
        self.results = {}
        if results:
            self.add_result(results)

    def add_result(self, result):
        if hasattr(result, 'keys'):
            self.results.update(result)
        elif hasattr(result, 'append'):
            for entry in result:
                self.add_result(entry)
        else:
            raise "Failed to add result: %s" % repr(result)

    def __repr__(self):
        import json
        return '''
Result: %s
  -> %s''' % (json.dumps(self.criteria, indent=4), json.dumps(self.results, indent=4), )
