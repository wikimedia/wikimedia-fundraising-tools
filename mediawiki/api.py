'''
Dumb interface to the MediaWiki api.
'''

import process.globals

import json


def mw_call(args, api=None):
    import simplemediawiki

    if not api:
        config = process.globals.get_config()
        api = config.centralnotice_mw_api

    wiki = simplemediawiki.MediaWiki(
        api,
        user_agent='bot: fr-anal'
    )
    result = wiki.call(args)
    if 'error' in result:
        raise RuntimeError(json.dumps(result, indent=4).replace('\\n', '\n'))
    val = result[args['action']]
    if 'list' in args:
        val = val[args['list']]
    return val
