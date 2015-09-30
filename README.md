Testing
=======

Use tox to execute linting/test scripts:

    tox

List of the default envs:

    tox -l

You can pass arguments to the underlying command with a double dash:

    tox -e flake8 -- --statistics
	tox -e flake8 -- ./audit
