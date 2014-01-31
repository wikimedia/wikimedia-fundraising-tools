from process.globals import config
from process.logging import Logger as log
import process.version_stamp

import os, os.path
import sys
import json
import socket
import time
from stompy import Stomp as DistStomp

class Stomp(object):
    conn = None

    def __init__(self):
        if not config.no_effect:
            self.conn = DistStomp(config.stomp.server, config.stomp.port)
            self.conn.connect()

    def __del__(self):
        if self.conn:
            self.conn.disconnect()

            # Let the STOMP library catch up
            import time
            time.sleep(1)

    def send(self, queue_key, body):
        if config.no_effect:
            log.info("not queueing message. " + json.dumps(body))
            return

        self.conn.send(self.create_message(queue_key, body))

    def create_message(self, queue_key, body):
        msg = {
            'destination': config.stomp.queues[queue_key],
            'persistent': 'true',
        } + Stomp.source_meta()

        if 'gateway' in body and 'gateway_txn_id' in body:
            msg['correlation-id'] = '{gw}-{id}'.format(gw=body['gateway'], id=body['gateway_txn_id'])

        msg.update({'body': json.dumps(body)})

        return msg

    @staticmethod
    def source_meta():
        return {
            'source_name': os.path.basename(sys.argv[0]),
            # FIXME: the controlling script should pass its own source_type
            'source_type': 'audit',
            'source_run_id': os.getpid(),
            'source_version': process.version_stamp.source_revision(),
            'source_enqueued_time': time.time(),
            'source_host': socket.gethostname(),
        }
