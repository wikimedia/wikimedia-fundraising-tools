import logging
import os
from yaml import safe_load as load_yaml

import process.log

_config = dict()
log = logging.getLogger(__name__)


def load_config(app_name):
    global _config

    search_filenames = [
        os.path.expanduser("~/.fundraising/%s.yaml" % app_name),
        os.path.expanduser("~/.%s.yaml" % app_name),
        # FIXME: relative path fail
        os.path.dirname(__file__) + "/../%s/config.yaml" % app_name,
        "/etc/fundraising/%s.yaml" % app_name,
        "/etc/%s.yaml" % app_name,
        # FIXME: relative path fail
        os.path.dirname(__file__) + "/../%s/%s.yaml" % (app_name, app_name,)
    ]
    # TODO: if getops.get(--config/-f): search_filenames.append

    for filename in search_filenames:
        if not os.path.exists(filename):
            continue

        _config = DictAsAttrDict(load_yaml(open(filename, 'r')))
        log.info("Loaded config from {path}.".format(path=filename))

        _config.app_name = app_name

        # TODO: Move up a level, entry point should directly call logging
        # configuration.
        process.log.setup_logging()

        return _config

    raise Exception("No config found, searched " + ", ".join(search_filenames))


def get_config():
    """Procedural way to get the config, to workaround early bootstrapping fluctuations"""
    return _config


class DictAsAttrDict(dict):
    def __getattr__(self, name):
        try:
            value = self[name]
            if isinstance(value, dict):
                value = DictAsAttrDict(value)
            return value
        except KeyError:
            raise AttributeError
