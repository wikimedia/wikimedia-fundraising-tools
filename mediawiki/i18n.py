from mediawiki.api import mw_call

def get_languages(**kw):
    result = mw_call({
        'action': 'query',
        'meta': 'siteinfo',
        'siprop': 'languages',
    }, **kw)
    return result['languages']
