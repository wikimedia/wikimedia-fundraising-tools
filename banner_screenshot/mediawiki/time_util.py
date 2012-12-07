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
