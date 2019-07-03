import http.client
import urllib.request
import urllib.error
import urllib.parse

import process.globals


class PaypalApiClassic(object):
    # See https://developer.paypal.com/docs/classic/release-notes/
    VERSION = '124.0'

    def call(self, cmd, **kw):
        config = process.globals.get_config()
        params = {
            'PWD': config.api.password,
            'USER': config.api.username,
            'METHOD': cmd,
            'VERSION': self.VERSION,
        }
        if 'signature' in config.api:
            params['SIGNATURE'] = config.api.signature

        params.update(kw)

        query = urllib.parse.urlencode(params)
        url = config.api.url + "?" + query

        req = urllib.request.Request(url)

        handlers = []

        # just for debugging DEBUGGING...
        # httplib.HTTPConnection.debuglevel = 3
        # httplib.HTTPSConnection.debuglevel = 3

        if 'certificate_path' in config.api:
            # handlers.append(HTTPSClientAuthHandler(config.api.certificate_path, config.api.certificate_path, debuglevel=2))
            handlers.append(HTTPSClientAuthHandler(config.api.certificate_path, config.api.certificate_path))

        opener = urllib.request.build_opener(*handlers)
        out = opener.open(req)

        result = urllib.parse.parse_qs(out.read())

        return result

    def fetch_donor_name(self, txn_id):
        response = self.call('GetTransactionDetails', TRANSACTIONID=txn_id)
        if 'FIRSTNAME' not in response:
            raise RuntimeError("Failed to get transaction details for {id}, repsonse: {response}".format(id=txn_id, response=response))
        return (response['FIRSTNAME'][0], response['LASTNAME'][0])


# from http://stackoverflow.com/questions/1875052/using-paired-certificates-with-urllib2
class HTTPSClientAuthHandler(urllib.request.HTTPSHandler):
    def __init__(self, key, cert, **kw):
        urllib.request.HTTPSHandler.__init__(self, **kw)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=20):
        return http.client.HTTPSConnection(host, key_file=self.key, cert_file=self.cert, timeout=timeout)
