'''
TODO:
* match start_time to discriminate between mutations of an otherwise identical test...
'''

import re

from fundraising_ab_tests.fundraising_test import FrTest
import campaign_log
from process.globals import config
from process.logging import Logger as log

def parse_spec(spec):
    for row in spec:
        yield FrTest(**row)

def compare_test_fuzzy(a, b):
    if a.campaign == b.campaign and a.banners == b.banners:
        return True

def is_fr_test(test):
    if test.label and test.banners and test.campaign:
        is_chapter = re.search(config.fr_chapter_test, test.banners[0])
        if is_chapter:
            log.debug("Determined banner {banner} belongs to a chapter".format(banner=test.banners[0]))
        else:
            log.debug("Determined banner {banner} belongs to Fundraising".format(banner=test.banners[0]))
        return not is_chapter
    log.warn("missing data for test {banner}".format(banner=test.banners[0]))


class FrTestSpec(object):
    '''Manage a collection of test specifications
    '''

    def __init__(self, spec=[]):
        self.spec = spec

    def update_test(self, test, insert=True):
        index = self.find_test(test)
        test.modified = True
        if index:
            self.spec[index] = test
        elif insert:
            self.spec.append(test)

    def find_test(self, test):
        for index, existing in enumerate(self.spec):
            if compare_test_fuzzy(test, existing):
                return index

    def update_from_logs(self):
        '''Try to reconstruct actual tests by parsing centralnotice change logs'''

        for test_ending, test_beginning in campaign_log.get_relevant_events():
            if test_ending:
                self.update_test(test_ending, insert=False)

            if test_beginning:
                if is_fr_test(test_beginning):
                    self.update_test(test_beginning)
