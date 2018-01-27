"""
Helper to make logging go to syslog and optionally stdout.
"""
import logging
import logging.config

import process.globals

_is_setup = False


def setup_logging():
    global _is_setup

    if _is_setup:
        return

    config = process.globals.get_config()

    logging.config.dictConfig(config.logging)

    _is_setup = True
