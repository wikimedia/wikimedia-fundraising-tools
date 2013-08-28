import re
from importlib import import_module
from yaml import safe_load as load_yaml

# n.b. Careful not to import `config` by value
config = dict()

def load_config(filename):
    global config

    if re.search(r'[.]py$', filename):
        config = import_module(filename[:-3])
    elif re.search(r'[.]ya?ml$', filename):
        config = DictAsAttrDict(load_yaml(file(filename, 'r')))
    else:
        raise Exception("No config found.")

class DictAsAttrDict(dict):
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value
