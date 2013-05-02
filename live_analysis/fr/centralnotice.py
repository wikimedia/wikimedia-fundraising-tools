'''
Interface to the MediaWiki CentralNotice api
'''

from mediawiki import mw_call

cached_campaigns = {}

def get_banners( **kw ):
    if 'campaign' in kw:
        campaign = get_campaign( kw['campaign'] )
        return campaign['banners'].keys()
    return get_allocations( **kw )

def get_campaign( campaign ):
    #TODO: push caching down into mediawiki.mw_call, with optional invalidation
    global cached_campaigns
    if campaign in cached_campaigns:
        return cached_campaigns[campaign]

    #if '__iter__' in campaign: return get_campaigns
    result = mw_call( {
        'action': 'centralnoticequerycampaign',
        'campaign': campaign,
    } )

    if campaign in result:
        result[campaign]['name'] = campaign
        cached_campaigns[campaign] = result[campaign]
        return cached_campaigns[campaign]

def get_campaigns( campaigns ):
    #FIXME cache
    return mw_call( {
        'action': 'centralnoticequerycampaign',
        'campaign': '|'.join( campaigns ),
    } )

def get_allocations( project=None, language=None, country=None, anonymous=True, bucket='0' ): 
    result = mw_call( {
        'action': 'centralnoticeallocations',
        'project': project,
        'language': language,
        'country': country,
        'anonymous': anonymous,
        'bucket': bucket,
        'minimal': 'false'
    } )
    return result['banners']

def get_campaign_logs( since=None, limit=50, offset=0 ):
    params = {
        'action': 'query',
        'list': 'centralnoticelogs',
        'limit': limit,
        'offset': offset,
    }
    if since:
        params['start'] = since

    result = mw_call( params )
    return result['logs']
