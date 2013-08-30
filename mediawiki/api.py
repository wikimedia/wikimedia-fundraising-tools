'''
Dumb interface to the MediaWiki api.
'''

from process.globals import config

import json

def mw_call( args ):
    import simplemediawiki

    wiki = simplemediawiki.MediaWiki(
        config.centralnotice_mw_api,
        user_agent='bot: fr-anal'
    )
    result = wiki.call( args )
    if 'error' in result:
        raise RuntimeError(json.dumps(result, indent=4).replace('\\n', '\n'))
    val = result[ args['action'] ]
    if 'list' in args:
        val = val[ args['list'] ]
    return val
