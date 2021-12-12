#! /bin/sh
# Calculates the IP ranges that sns.us-east-1.amazonaws.com may resolve to
# by looking up the full AWS ip range list then limiting it to only the
# us-east-1 ranges used for Amazon services.
IPFILE=`mktemp`
INCLUDEFILE=`mktemp`
EXCLUDEFILE=`mktemp`
MYPATH=`dirname $0`
wget --quiet https://ip-ranges.amazonaws.com/ip-ranges.json -O $IPFILE
jq '.prefixes[] | select(.region|startswith("us-east-1")) | select(.service=="AMAZON") | .ip_prefix' $IPFILE \
    | sed -e 's/"//g' > $INCLUDEFILE
jq '.prefixes[] | select(.service!="AMAZON") | .ip_prefix' $IPFILE \
    | sed -e 's/"//g' > $EXCLUDEFILE
$MYPATH/netdiff.py $INCLUDEFILE $EXCLUDEFILE
rm $IPFILE $INCLUDEFILE $EXCLUDEFILE
