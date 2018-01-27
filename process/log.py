"""
Helper to make logging go to syslog and optionally stdout.

TODO:
* Deprecate Logger and use the usual pattern:
  log = logging.getLogger(__name__)
"""
import logging
import logging.config

import process.globals

_is_setup = False
# TODO: Modules should do this internally, so logging happens under the correct
# package name and can be filtered.
_log = logging.getLogger(__name__)


# Deprecated.
class Logger(object):
    @staticmethod
    def debug(message):
        _log.debug(message)

    @staticmethod
    def info(message):
        _log.info(message)

    @staticmethod
    def warn(message):
        _log.warning(message)

    @staticmethod
    def error(message):
        _log.error(message)

    @staticmethod
    def fatal(message):
        _log.critical(message)


def setup_logging():
    global _is_setup

    if _is_setup:
        return

    config = process.globals.get_config()

    logging.config.dictConfig(config.logging)

    _is_setup = True
