from process.globals import config
from process.logging import Logger as log

import json
import redis

class Redis(object):

    conn = None

    def __init__(self):
        if not config.no_effect:
            self.conn = redis.Redis(host=config.redis.server, port=config.redis.port, password=config.redis.password)

    def send(self, queue, msg):

        if config.no_effect:
            log.info("not queueing message. " + json.dumps(msg))
            return

        if config.redis.queues[queue]:
            self.conn.rpush(config.redis.queues[queue], msg)
        else:
            self.conn.rpush(queue, msg)
