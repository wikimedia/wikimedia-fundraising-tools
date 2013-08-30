import re
import json

from process.globals import config
from mediawiki.centralnotice.contributions import get_totals
from mediawiki.centralnotice.impressions import get_impressions
from fundraising_ab_tests.confidence import add_confidence

class TestResult(object):
    def __init__(self, criteria=None, results={}):
        self.criteria = criteria
        self.results = results

    def __repr__(self):
        return '''
Result: %s
  -> %s''' % (
            json.dumps(self.criteria, indent=4),
            json.dumps(self.results, indent=4),
        )


def get_banner_results(cases):
    results = [ banner_results(case) for case in cases ]

    add_confidence(results, 'banner', 'donations')

    return results

def banner_results(criteria):
    results = get_totals(**criteria)
    impressions = get_impressions(**criteria)

    results.update({
        'preview': "http://en.wikipedia.org/wiki/Special:Random?banner=%s&reset=1" % criteria['banner'],
        'screenshot': "http://fundraising-archive.wmflabs.org/banner/%s.png" % criteria['banner'],

        'impressions': str(impressions),
    })

    # FIXME: refactor to a variations hook
    match = re.match(config.fr_banner_naming, criteria['banner'])
    if match:
        results.update({
            'label': match.group("testname"),
            'language': match.group("language"),
            'variation': match.group("variation"),
            'dropdown': match.group("dropdown") is "dr",
            'country': match.group("country"),

            'preview': "http://en.wikipedia.org/wiki/Special:Random?banner=%s&country=%s&uselang=%s&reset=1" % (criteria['banner'], match.group("country"), match.group("language")),
        })

    return TestResult(criteria, results)
