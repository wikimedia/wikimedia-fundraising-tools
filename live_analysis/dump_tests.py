#!/usr/bin/env python

"""Script to dump campaign logs to a file
"""

import csv
import sys

import mediawiki.centralnotice.api

from process.globals import load_config

load_config("analysis")


def is_relevant(entry):
    '''
    This change enabled/disabled a test; or an enabled test has been mutated.
    '''
    if not entry['end']['banners']:
        return False

    if 'enabled' in entry['added'] or entry['begin']['enabled'] is 1:
        return True


def fetch():
    out = csv.DictWriter(sys.stdout, [
        'campaign',
        'banner',
        'start',
        'end',
        # FIXME: 'lps',
    ], delimiter="\t")

    out.writeheader()

    cur = 0
    pagesize = 500

    while True:
        logs = mediawiki.centralnotice.api.get_campaign_logs(limit=pagesize, offset=cur)

        for test in logs:
            if is_relevant(test):
                for banner in test['end']['banners'].keys():
                    out.writerow({
                        'campaign': test['campaign'].encode('utf-8'),
                        'banner': banner.encode('utf-8'),
                        'start': test['end']['start'],
                        'end': test['end']['end'],
                    })

        if not logs:
            break

        cur = cur + pagesize

if __name__ == "__main__":
    fetch()
