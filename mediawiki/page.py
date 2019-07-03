from mediawiki.api import mw_call


def get_content(title, **kw):
    result = mw_call({
        'action': 'query',
        'prop': 'revisions',
        'titles': title,
        'rvprop': 'content',
    }, **kw)
    page = list(result['pages'].values()).pop()
    if 'revisions' in page:
        revision = page['revisions'].pop()
        return revision['*']
    else:
        return None
