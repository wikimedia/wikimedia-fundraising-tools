Testing
=======

Use tox to execute linting/test scripts:

    tox

List of the default envs:

    tox -l

You can pass arguments to the underlying command with a double dash:

    tox -e flake8 -- --statistics
    tox -e flake8 -- ./audit

### Test database
Silverpop tests require a test mysql database to exist. Add it with the following:
1. create mysql dastabase ```test```
2. create user ```CREATE USER 'test'@'localhost';```
3. grant privileges ```GRANT ALL PRIVILEGES ON test.* TO 'test'@'localhost';```

### For Vagrant Users
Unfortunately tox doesn't work with vagrant nfs mounts. If you use mediawiki-vagrant on linux then you will most likely be unable to run tox.

To work around this, you can just install the project dependencies using:

`pip install -r requirements.txt -r test-requirements.txt`

And then run nosetests directly from the project root by calling:

`python -m nose`




