import mediawiki.centralnotice.api
from process.logging import Logger as log
from process.globals import config
from results import get_banner_results

class FrTest(object):
    def __init__(self, label=None, type="", campaign=None, banners=None, start=None, end=None, disabled=False, **ignore):
        for key in config.ignored_columns:
            if key in ignore:
                ignore.pop(key)
        if ignore:
            log.warn("ignoring columns: {columns}".format(columns=", ".join(ignore.keys())))

        self.campaign = mediawiki.centralnotice.api.get_campaign(campaign)
        if not self.campaign:
            log.warn("no such campaign '{campaign}'".format(campaign=campaign))

        self.type = type.lower()

        self.banners = []
        if self.type.count('banner') > 0:
            self.is_banner_test = True
            if banners:
                if hasattr(banners, 'strip'):
                    banners = [ s.strip() for s in banners.split(",") ]
                self.banners = banners
            else:
                if self.campaign['banners']:
                    self.banners = self.campaign['banners'].keys()

            #self.variations = [ FrTestVariation(banner=name) for name in self.banners ]

        self.is_country_test = (self.type.count('country') > 0)
        self.is_lp_test = (self.type.count('lp') > 0)

        self.start_time = start
        self.end_time = end

        self.label = label
        if not self.label:
            # FIXME
            self.label = campaign

        self.enabled = not disabled

        self.results = []

    def load_results(self):
        if self.is_banner_test and self.banners:
            cases = []
            for name in self.banners:
                test_case = self.get_case(
                    campaign=self.campaign['name'],
                    banner=name
                )
                cases.append(test_case)

            self.results.extend(get_banner_results(cases))

        if self.is_country_test:
            #results = [ calculate_result(country=code) for code in campaign['countries'] ]
            #self.results.extend(results)
            log.warn("country test type not implemented")

        if self.is_lp_test:
            log.warn("LP test type not implemented")

    def get_case(self, **kw):
        conditions = {
            'start': self.start_time,
            'end': self.end_time,
        }
        conditions.update(**kw)

        return conditions

    def __repr__(self):
        description = '''
Test: %(label)s (%(campaigns)s) %(start)s - %(end)s
''' % {
            'label': self.label,
            'campaigns': self.campaign['name'],
            'start': self.start_time,
            'end': self.end_time,
        }
        if not self.enabled:
            description += " DISABLED "
        if self.is_banner_test:
            description += " banners: " + str(self.banners)
        if self.is_country_test:
            description += " countries: " + str(self.countries)
        if self.is_lp_test:
            description += " lps: " + str(self.lps)
        return description

#class FrTestVariation(object):
