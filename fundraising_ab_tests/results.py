import json
from process.globals import config
from mediawiki.centralnotice.contributions import get_totals
from mediawiki.centralnotice.impressions import get_impressions
from fundraising_ab_tests.confidence import add_confidence


class TestResult(object):
    """Container for a test's results

    TODO: fix single-responsibility issue with criteria"""
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
    results = [banner_results(case) for case in cases]

    add_confidence(results, 'banner', 'donations')

    return results


def banner_results(criteria):
    """Helper which retrieves performance statistics for the given test criteria"""
    results = get_totals(**criteria)
    impressions = get_impressions(**criteria)

    # FIXME: refactor to a variations hook
    # match = re.match(config.fr_banner_naming, criteria['banner'])
    # if match:
    #     results.update({
    #         'label': match.group("testname"),
    #         'language': match.group("language"),
    #         'variation': match.group("variation"),
    #         'dropdown': match.group("dropdown") is "dr",
    #         'country': match.group("country"),

    #     })

    # Get example locales, to help generate valid links
    language = criteria['languages'][0]
    if criteria['countries']:
        country = criteria['countries'][0]
    else:
        country = 'US'

    results.update({
        'preview': config.preview_format.format(banner=criteria['banner'], language=language, country=country),
        'screenshot': config.screenshot_format.format(banner=criteria['banner'], language=language),

        'impressions': str(impressions),
    })

    return TestResult(criteria, results)
