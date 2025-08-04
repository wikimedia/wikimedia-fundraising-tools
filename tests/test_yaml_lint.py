from glob import glob
import os

import pytest
import yaml

_basedir = os.path.realpath(os.path.join(__file__, '..', '..'))
yaml_files = (
    glob(os.path.join(_basedir, '**/*.yaml.example'), recursive=True)
    + glob(os.path.join(_basedir, '**/*.yaml'), recursive=True)
)


@pytest.mark.parametrize("yaml_file", yaml_files)
def test_is_valid_yaml(yaml_file):
    with open(yaml_file, 'r') as f:
        yaml.safe_load(f)
