import os.path
from yaml import safe_load as load_yaml

from process.logging import Logger as log

# n.b. Careful not to import `config` by value.  TODO: rename to _config to
# break external usages.
config = dict()


def load_config(app_name):
    global config

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

        config = DictAsAttrDict(load_yaml(file(filename, 'r')))
        log.info("Loaded config from {path}.".format(path=filename))

        config.app_name = app_name

        return config

    raise Exception("No config found, searched " + ", ".join(search_filenames))


def get_config():
    """Procedural way to get the config, to workaround early bootstrapping fluctuations"""
    global config
    return config


class DictAsAttrDict(dict):
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsAttrDict(value)
        return value
