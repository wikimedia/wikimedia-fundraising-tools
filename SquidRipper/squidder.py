#!/usr/bin/python

from optparse import OptionParser
import httplib
import urllib
import os
import fnmatch
import json

# NOTE: This script requires a set of WMF PyBal configuration files. These may be obtained
# from fenari:/h/w/conf/pybal.conf. For more details talk to paravoid.


def main():
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-p", "--project", dest='project', default='wikipedia', help='')
    parser.add_option("-l", "--language", dest='language', default='en', help='')
    parser.add_option("-c", "--country", dest='country', default='US', help='')
    parser.add_option("-a", "--anon", dest='anon', action='store_true', default=False, help='')
    parser.add_option("-d", "--device", dest='device', default='desktop', help='')
    parser.add_option("-b", "--bucket", dest='bucket', default=0, help='')
    parser.add_option("-s", "--slots", dest='slots', default=30, help='')

    (options, args) = parser.parse_args()

    # Get all the hosts we're connecting to!
    if len(args) > 0:
        hosts = args
    else:
        hosts = set(loadPybal('text'))
        hosts.union(set(loadPybal('text-squids')))
        hosts.union(set(loadPybal('text-varnish')))
        hosts = list(hosts)
        hosts.sort()

    # Construct the URL set
    headers = {
        'Host': 'meta.wikimedia.org',
        'User-Agent': 'CentralNotice-slot-verification-bot mwalker@wikimedia.org'
    }

    params = []
    for slot in range(1, options.slots + 1):
        params.append(
            '/wiki/Special:BannerRandom?' + urllib.urlencode({
                'uselang': options.language,
                'project': options.project,
                'anonymous': str(options.anon).lower(),
                'bucket': options.bucket,
                'country': options.country,
                'device': options.device,
                'slot': slot
            })
        )

    # Assume the first server is authoritative and get what's in it's slots
    authBanners = getSlotContents(hosts[0], headers, params)

    print("Banners currently in %s" % hosts[0])
    print("Example URL: %s" % params[0])
    for i in range(0, len(authBanners)):
        print("%02d: %s" % (i + 1, authBanners[i]))
    print("\n")

    # Now do the rest of them
    if len(hosts) > 1:
        for i in range(1, len(hosts)):
            print("Getting %s" % hosts[i])
            testBanners = getSlotContents(hosts[i], headers, params)
            compareBanners(authBanners, testBanners, hosts[i])

    print("Done!")


def loadPybal(pattern):
    files = []
    for root, dirnames, filenames in os.walk('pybal'):
        for filename in fnmatch.filter(filenames, pattern):
            files.append(os.path.join(root, filename))

    lines = []
    for fname in files:
        with open(fname) as f:
            lines.extend(f.readlines())

    hosts = []
    for line in lines:
        try:
            obj = eval(line)
            hosts.append(obj['host'])
        except Exception:
            pass

    return hosts


def getSlotContents(host, headers, urls):
    conn = httplib.HTTPConnection(host)

    contents = []
    for url in urls:
        conn.request('GET', url, '', headers)
        result = conn.getresponse()
        try:
            resultStr = result.read()[31:-3]
            jobj = json.loads(resultStr)
            contents.append(jobj['bannerName'])
        except Exception:
            contents.append(None)
    return contents


def compareBanners(auth, test, host):
    if len(auth) != len(test):
        print "Response lengths not equal from %s" % host
        return

    for i in range(0, len(test)):
        if auth[i] != test[i]:
            print("Slot %02d from %s contains %s" % (i + 1, host, test[i]))


if __name__ == "__main__":
    main()
