from process.globals import config

import urllib
import urllib2
import urlparse

class PaypalApiClassic(object):
    VERSION = '98.0' # pseudo-random guess

    def call(self, cmd, **kw):
        params = {
            'PWD': config.api.password,
            'USER': config.api.username,
            'METHOD': cmd,
            'VERSION': self.VERSION,
        }
        if 'signature' in config.api:
            params['SIGNATURE'] = config.api.signature
        elif 'certificate_path' in config.api:
            # TODO
            pass

        params.update(kw)

        query = urllib.urlencode(params)
        url = config.api.url + "?" + query

        req = urllib2.Request(url)
        out = urllib2.urlopen(req)

        result = urlparse.parse_qs(out.read())

        return result
