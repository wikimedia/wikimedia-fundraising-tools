from database import db
from process.globals import get_config


def get_impressions(campaign=None, banner=None, **ignore):
    config = get_config()
    query = db.Query()
    query.columns.append("SUM(count) AS count")
    query.tables.append("{impressions_db}bannerimpressions".format(impressions_db=config.impressions_prefix))
    if campaign:
        query.where.append("campaign = %(campaign)s")
        query.params['campaign'] = campaign
    if banner:
        query.where.append("banner = %(banner)s")
        query.params['banner'] = banner

    result = list(db.get_db().execute(query))
    row = result.pop()

    return row['count']
