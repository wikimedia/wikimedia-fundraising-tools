import re
import os.path
from yaml import safe_load as load_yaml

# n.b. Careful not to import `config` by value
config = dict()

def load_config(app_name):
    global config

    search_filenames = [
        os.path.expanduser("~/.fundraising/%s.yaml" % app_name),
        os.path.expanduser("~/.%s.yaml" % app_name),
        "config.yaml",
        "/etc/fundraising/%s.yaml" % app_name,
        "/etc/%s.yaml" % app_name,
        "%s.yaml" % app_name,
    ]
    # TODO: if getops.get(--config/-f): search_filenames.append

    for filename in search_filenames:
        if not os.path.exists(filename):
            continue

        config = DictAsAttrDict(load_yaml(file(filename, 'r')))

        return

    raise Exception("No config found, searched " + ", ".join(search_filenames))

class DictAsAttrDict(dict):
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsAttrDict(value)
        return value
