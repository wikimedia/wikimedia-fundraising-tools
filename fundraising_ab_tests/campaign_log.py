from fundraising_ab_tests.fundraising_experiment import FrTest
from process.globals import config

def tests_from_entry(entry):
    '''
    Returns a tuple, (test ended, test begun)

    Start and end times are fudged quite a bit, all we care about
    for now is defining broad enough criteria to capture most of
    the related contributions.
    '''
    def test_from_entry(edge):
        if (edge == 'begin' and 'enabled' in entry['added'] and int(entry['added']['enabled'])) or (edge == 'end' and 'enabled' in entry['removed'] and int(entry['removed']['enabled'])):
            # irrelevant
            return False

        # FIXME: assuming this is a banner test
        banners = entry[edge]['banners']
        if hasattr(banners, 'keys'):
            banners = banners.keys()

        start = entry[edge]['start']
        if edge == 'end':
            start = entry['timestamp']

        end = entry[edge]['end']
        if edge == 'begin':
            end = entry['timestamp']

        return FrTest(
            type="banner",
            campaign=entry['campaign'],
            banners=banners,
            start=start,
            end=end
        )

    return (test_from_entry('begin'), test_from_entry('end'), )

def get_relevant_events():
    from mediawiki.centralnotice.api import get_campaign_logs
    from mediawiki.centralnotice import time_util

    def is_relevant(entry):
        '''
        This change enabled/disabled a test; or an enabled test has been mutated.
        '''
        if 'enabled' in entry['added'] or entry['begin']['enabled'] is 1:
            return True

    logs = get_campaign_logs(since=time_util.str_time_offset(days=-config.centralnotice_history_days))
    return [ tests_from_entry(e) for e in reversed(logs) if is_relevant(e) ]
