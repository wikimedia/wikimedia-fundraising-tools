#!/usr/bin/python2

import re
import sys
from optparse import OptionParser
import socket
import struct

# Regex based on http://wikitech.wikimedia.org/view/Squid_log_format
cacheRegex = re.compile(
    r"""
        ^(?P<server>[\S]+) # Name of the squid server
        \s[-]*
        (?P<sequence>[0-9]+) # Sequence ID from the squid server
        \s
        (?P<timestamp>[0-9-]+T[0-9:.]+) # Timestamp
        \s
        (?P<servicetime>[0-9.]+) # Request service time
        \s
        (?P<client>[\S]+) # Client IP address
        \s
        (?P<httpstatus>[\S]+) # Squid request status and HTTP status code
        \s
        (?P<replysize>[0-9]+) # Reply size including HTTP headers
        \s
        (?P<httpmethod>[\S]+) # Request type
        \s
        (?P<url>[\S]+) # Request URL
        \s
        (?P<squidhierarchy>[\S]+) # Squid hierarchy status, peer IP
        \s
        (?P<mime>[\S]+) # MIME content type
        \s
        (?P<referrer>[\S]+) # Referer header
        \s
        (?P<xff>[\S]+) # X-Forwarded-For header
        \s
        (?P<useragent>[\S\s]+) # User-Agent header
        \s
        (?P<acceptlanguage>[\S\s]+) # Accept-Language header
        \s
        .*$
    """, re.VERBOSE
)

# Based on urlparse.urlsplit which is really slow but only does:
# <scheme>://<netloc>/<path>?<query>#<fragment>
# This regex does not replicate all functionality, just optimizes
# even further for our purposes
urlRegex = re.compile(
    r"""
        (?P<urlscheme>http|https)
        ://
        (?P<urlhost>(?:(?!/|\?|\#)\S)*)
        /?
        (?P<urlpath>(?:(?!\?|\#)\S)*)
        \??
        (?P<urlquery>(?:(?!\#)\S)*)
        \#?
        (?P<urlfragment>[\S]*)
    """, re.VERBOSE
)

# Ignore these subnets on request
localSubnets = [
    '10.0.0.0/8',
    '208.80.152.0/22',
    '208.80.155.0/27',
    '91.198.174.0/24',
]


def initIpFilter():
    global localSubnets
    subnets = []
    for subnet in localSubnets:
        (network, mask) = subnet.split('/')
        network = struct.unpack('L', socket.inet_aton(network))[0]
        mask = (2L << mask - 1) - 1
        subnets.append((mask, network))
    localSubnets = subnets


def ipFilter(squidParts):
    global localSubnets

    ip = struct.unpack('L', socket.inet_aton(squidParts['client']))[0]
    for subnet in localSubnets:
        if ip & subnet[0] == subnet[1]:
            return True
    return False


def sslFilter(squidParts):
    if squidParts['server'][:3] == 'ssl':
        return True
    else:
        return False


def getSquidParts(line):
    match = cacheRegex.match(line)
    if match:
        return match.groupdict()
    else:
        raise Exception("Cache regex did not match: %s" % line)


def getHttpRequest(squidParts):
    match = urlRegex.match(squidParts['url'])
    if match:
        return match.groupdict()
    else:
        raise Exception("URL regex did not match: %s" % squidParts['url'])


def getHttpParams(httpParts):
    elements = httpParts['urlquery'].split('&')
    elementDict = {}
    for element in elements:
        try:
            (k, v) = element.split('=')
        except ValueError:
            k = element
            v = None
        elementDict[k] = v
    return elementDict


if __name__ == "__main__":
    parser = OptionParser(usage="usage: %prog [options] <columns...>")
    parser.add_option(
        "-i", "--filterIP", dest='filterIP', action='store_true', default=False, help='Filter out local IPs'
    )
    parser.add_option(
        "-s", "--filterSSL", dest='filterSSL', action='store_true', default=False,
        help='Filter out initial SSL connections'
    )
    (options, args) = parser.parse_args()

    if options.filterIP:
        initIpFilter()

    # Main application loop
    for line in sys.stdin:
        try:
            squidParts = getSquidParts(line)
            if options.filterIP and ipFilter(squidParts):
                continue
            if options.filterSSL and sslFilter(squidParts):
                continue

            # OK we're supposed to be here, now do the more expensive matching
            httpParts = getHttpRequest(squidParts)
            urlParts = getHttpParams(httpParts)

            # And now compose the output line
            out = []
            for k in args:
                if k in squidParts:
                    out.append(squidParts[k])
                elif k in httpParts:
                    out.append(httpParts[k])
                elif k in urlParts:
                    out.append(urlParts[k])
                else:
                    out.append('-')  # When we cannot otherwise find anything
            sys.stdout.write(' '.join(out))
            sys.stdout.write("\n")

        except Exception as e:
            sys.stderr.write("Could not process %s" % e.message)
