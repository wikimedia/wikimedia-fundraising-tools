'''
Aggregate contribution data.

TODO: Use anonymized tables
'''

import decimal

from process.globals import config
from database import db

import time_util

ct_banner_clause = "LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(utm_source, '.', 2),'.',1), LENGTH(SUBSTRING_INDEX(SUBSTRING_INDEX(utm_source, '.', 2),'.',1)))"


def get_change(wheres):
    begin = time_util.str_time_offset(minutes=-90)
    end = time_util.str_time_offset(minutes=-30)

    ref_begin = time_util.same_time_another_day(config.reference_day, begin)
    ref_end = time_util.same_time_another_day(config.reference_day, end)

    cur_totals = get_totals(wheres, start=begin, end=end)
    ref_totals = get_totals(wheres, start=ref_begin, end=ref_end)

    change = None
    if ref_totals['total']:
        change = round(100 * cur_totals['total'] / ref_totals['total'] - 100, 1)
    cur_totals['frac_change'] = change

    return cur_totals


# FIXME: instead of ignoring args, intersect the criteria during update
def get_totals(wheres=None, query=None, banner=None, campaign=None, country=None, start=None, end=None, **ignore):
    '''
    Note that the column names must match a heading in the results spreadsheet.
    '''
    if not query:
        query = db.Query()
    query.columns.append('SUM(total_amount) AS total')
    query.columns.append('AVG(total_amount) AS mean')
    query.columns.append('AVG( IF(total_amount > 20, 20, total_amount) ) AS mean20')
    query.columns.append('STD(total_amount) AS sd')
    query.columns.append('COUNT(ct.id) AS clicks')
    query.columns.append('COUNT(cc.id) AS donations')

    query.tables.append(config.contribution_tracking_prefix + 'contribution_tracking ct')
    query.tables.append("civicrm_contribution cc ON cc.id = ct.contribution_id")

    if wheres:
        query.where.extend(wheres)
    if campaign:
        query.columns.append("utm_campaign AS campaign")
        query.where.append("utm_campaign = %(campaign)s")
        query.params['campaign'] = campaign
    if banner:
        query.columns.append(ct_banner_clause + " AS banner")
        query.where.append(ct_banner_clause + " = %(banner)s")
        query.params['banner'] = banner
    if country:
        query.where.append("country = %(country)s")
        query.params['country'] = country
    if start and end:
        query.where.append("ts BETWEEN %(start)s AND %(end)s")
        query.params['start'] = start
        query.params['end'] = end

    if not query.where:
        raise Exception("Don't try this query without a where clause.")

    result = list(db.get_db().execute(query))
    row = result.pop()

    # nasty hack for json encoding snafu:
    for k, v in row.items():
        if isinstance(v, decimal.Decimal):
            row[k] = str(v)

    # add some goodies:
    row['sql'] = str(query)
    row['updated'] = time_util.str_now()

    return row
