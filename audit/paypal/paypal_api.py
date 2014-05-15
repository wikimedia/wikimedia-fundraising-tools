from process.globals import config

import httplib
import urllib
import urllib2
import urlparse


class PaypalApiClassic(object):
    # pseudo-random guess
    VERSION = '98.0'

    def call(self, cmd, **kw):
        params = {
            'PWD': config.api.password,
            'USER': config.api.username,
            'METHOD': cmd,
            'VERSION': self.VERSION,
        }
        if 'signature' in config.api:
            params['SIGNATURE'] = config.api.signature

        params.update(kw)

        query = urllib.urlencode(params)
        url = config.api.url + "?" + query

        req = urllib2.Request(url)

        handlers = []

        # just for debugging DEBUGGING...
        #httplib.HTTPConnection.debuglevel = 3
        #httplib.HTTPSConnection.debuglevel = 3

        if 'certificate_path' in config.api:
            #handlers.append(HTTPSClientAuthHandler(config.api.certificate_path, config.api.certificate_path, debuglevel=2))
            handlers.append(HTTPSClientAuthHandler(config.api.certificate_path, config.api.certificate_path))

        opener = urllib2.build_opener(*handlers)
        out = opener.open(req)

        result = urlparse.parse_qs(out.read())

        return result

# from http://stackoverflow.com/questions/1875052/using-paired-certificates-with-urllib2

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert, **kw):
        urllib2.HTTPSHandler.__init__(self, **kw)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=20):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert, timeout=timeout)
