'''
TODO:
* match start_time to discriminate between mutations of an otherwise identical test...
'''

import re

from fr.tests import FrTest
import campaign_log

def parse_spec(spec):
    for index, row in enumerate(spec):
        if 'disabled' in row and hasattr(row['disabled'], 'strip') and row['disabled'].strip():
            #print "DEBUG: Skipping disabled test spec: %s" % str(row)
            continue
        yield FrTest(source_index=index, **row)

def compare_test_fuzzy(a, b):
    if a.campaigns == b.campaigns and a.banners == b.banners:
        return True

def is_fr_test(test):
    if test.label and test.banners and test.campaigns:
        is_chapter = re.search(r'(_|\b)WM[A-Z]{2}(_|\b)', test.banners[0])
        return not is_chapter


class FrTestSpec(object):
    '''Manage a collection of test specifications
    '''

    def __init__(self, spec=[]):
        self.spec = spec

    def update_test(self, test, insert=True):
        index = self.find_test(test)
        test.modified = True
        if index:
            test.source_index = self.spec[index].source_index
            self.spec[index] = test
        elif insert:
            if self.spec:
                test.source_index = self.spec[-1].source_index + 1
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
