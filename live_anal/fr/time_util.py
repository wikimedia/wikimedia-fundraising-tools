'''
Time utilities, originally from http://svn.wikimedia.org/svnroot/wikimedia/branches/deployment/fundraiser-analysis
'''
from datetime import datetime, timedelta

def str_time_offset(str_time=None, **delta_args):
    if not str_time:
        str_time = str_now()
    time_time = datetime.strptime( str_time, '%Y%m%d%H%M%S' )
    str_time = ( time_time + timedelta( **delta_args )).strftime( '%Y%m%d%H%M%S' )
    return(str_time)

def str_now():
    return( datetime.utcnow().strftime('%Y%m%d%H%M%S') )

def datetimefunix( unix_timestamp ):
    return datetime.fromtimestamp(unix_timestamp)

def strfunix( unix_timestamp ):
    return datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M')

def same_time_another_day(ref_day, time):
    return ref_day[:8] + time[-6:]
