import os.path
import yaml


def test_valid_yaml():
    basedir = os.path.realpath(os.path.join(__file__, '..', '..'))
    yaml_files = []
    for dirpath, dirnames, filenames in os.walk(basedir):
        yaml_files.extend(
            [os.path.join(dirpath, f) for f in filenames
             if f.endswith('.yaml') or f.endswith('.yaml.example')]
        )
    for yaml_file in yaml_files:
        yield is_valid_yaml, yaml_file


def is_valid_yaml(filename):
    with open(filename, 'r') as f:
        yaml.safe_load(f)
