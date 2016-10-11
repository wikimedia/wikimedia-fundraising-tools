import os
import process.globals
import process.version_stamp
import socket
import sys
import time
from process.logging import Logger as log

import json
import redis


class Redis(object):

    conn = None
    config = process.globals.get_config()

    def __init__(self):
        if not self.config.no_effect:
            self.conn = redis.Redis(host=self.config.redis.server, port=self.config.redis.port, password=self.config.redis.password)

    def send(self, queue, msg):

        encoded = json.dumps(msg)

        if self.config.no_effect:
            log.info("not queueing message. " + encoded)
            return

        msg.update(Redis.source_meta())

        if queue in self.config.redis.queues:
            # Map queue name if desired.
            self.conn.rpush(self.config.redis.queues[queue], encoded)
        else:
            self.conn.rpush(queue, encoded)

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
