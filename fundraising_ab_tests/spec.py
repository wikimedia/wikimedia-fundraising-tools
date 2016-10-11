"""
Specification for a list of tests

TODO:
* lots of refinement and clarification around test vs spec
* match start_time to discriminate between mutations of an otherwise identical test...
"""

import re

from fundraising_ab_tests.fundraising_experiment import FrTest
import campaign_log
from process.globals import config
from process.logging import Logger as log


def parse_spec(spec):
    """Turn each row of a specification source into test objects"""
    for row in spec:
        yield FrTest(**row)


def compare_test_fuzzy(a, b):
    """Check whether the tests match closely enough to be considered identical."""
    if a.campaign['name'] == b.campaign['name'] and a.banners == b.banners:
        return True


def is_fr_test(test):
    if test.label and test.banners and test.campaign:
        is_chapter = re.search(config.fr_chapter_test, test.banners[0])
        if is_chapter:
            log.debug("Determined test {title} belongs to a chapter".format(title=test.label))
        else:
            log.debug("Determined test {title} belongs to Fundraising".format(title=test.label))
        return not is_chapter

    log.warn("missing data for test {title}".format(title=test.label))


class FrTestSpec(object):
    '''Manage a collection of test specifications'''

    def __init__(self, spec=[]):
        self.spec = spec

    def update_test(self, test, insert=True):
        """If the test is already listed, replace it.  Otherwise, append."""
        index = self.find_test(test)
        test.modified = True
        if index is not None:
            self.spec[index] = test
        elif insert:
            self.spec.append(test)

    def find_test(self, test):
        """Search for a given test in the specification so far.  If it exists, return the list index."""
        for index, existing in enumerate(self.spec):
            if compare_test_fuzzy(test, existing):
                return index

    def update_from_logs(self):
        '''Try to reconstruct actual tests by parsing centralnotice change logs'''
        # FIXME: bad things happen here.  We're currently picking up one extra test duplicate  at the end of the list.

        for test_ending, test_beginning in campaign_log.get_relevant_events():
            if test_ending:
                self.update_test(test_ending, insert=False)

            if test_beginning:
                if is_fr_test(test_beginning):
                    self.update_test(test_beginning)
