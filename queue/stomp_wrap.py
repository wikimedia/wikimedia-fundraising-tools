from process.globals import config
from process.logging import Logger as log

import json
from stompy import Stomp as DistStomp

class Stomp(object):
    conn = None

    def __init__(self):
        self.conn = DistStomp(config.stomp.server, config.stomp.port)
        self.conn.connect()

    def __del__(self):
        if self.conn:
            self.conn.disconnect()

            # Let the STOMP library catch up
            import time
            time.sleep(1)

    def send(self, msg, queue_key):
        if config.no_effect:
            log.info("not queueing message. " + json.dumps(msg))
            return

        meta = {
            'destination': config.stomp.queues[queue_key],
            'persistent': 'true',
        }

        if 'gateway' in msg and 'gateway_txn_id' in msg:
            meta['correlation-id'] = '{gw}-{id}'.format(gw=msg['gateway'], id=msg['gateway_txn_id'])

        #log.debug("sending %s %s" % (meta, msg))

        meta.update({'body': json.dumps(msg)})
        self.conn.send(meta)
