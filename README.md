Documentation
========
The main documentation for our Acoustic exports can be found at
https://wikitech.wikimedia.org/wiki/Fundraising/Data_and_Integrated_Processes/Acoustic_Integration#Exporting_data_to_Acoustic

Status
========
We don't have a fully sorted config to make this 'just work' on our docker installs
with no set up. The current place for work on that is https://phabricator.wikimedia.org/T341017
This README provides enough to run the code in 2 ways
- the test script (tox) which uses a mock civicrm database called 'test' created by loading the
  (manually updated - sigh) tests\minimal_schema.sql
- a 'live' script which runs off your 'live' civicrm database and updates the `silverpop`
  database.

In Production
=============
The Acoustic (Silverpop) export code runs on our staging server nightly using our replica
db.

Testing
=======

To run the test script you need to run the command `tox` in this folder on your civicrm
container. The steps below describe config/ cli options to get it to run.

### Test database
Silverpop tests require a test mysql database to exist. Add it with the following:
1. ```CREATE database test;```
2. ```CREATE USER `test`@`%`;```
3. ```GRANT ALL PRIVILEGES ON test.* TO `test`@`%`;```

### For docker users
From the civicrm container
it is enough to login as root (bkr - see aliases below) and
1) apt update
2) apt install python3-pip
3) apt install tox
4) login (bkb  - see aliases below), navigate to this directory (/srv/tools/silverpop_export) & run `tox`

## If you have just applied a patch that changes the schema....

run drop_incremental_schema.sql

The tables will be recreated if they don't exist - but not updated if they
do. If you wish to recreate them manually call rebuild_schema.sql

## Smoke testing on your local
To run the 'live' script from inside the docker civicrm container you likely need to
create the database first ie

in mysql
```
CREATE DATABASE silverpop;
```

You may also need to install the requirements first (in theory running `tox` might do this for you) - ie

```
pip3 install -r /srv/tools/requirements.txt
```

```
env PYTHONPATH=/srv/tools  LOGNAME=docker /srv/tools/silverpop_export/update.py
```

After running this you can check the silverpop database is correctly populated.

You can also generate the csv files

```
env PYTHONPATH=/srv/tools  LOGNAME=docker /srv/tools/silverpop_export/update.py
```

It is also possible to run tox and the update script from your local host. Last time I
updated this file I was only getting the local host to run the live script. As of this update
it won't run but the above variant of running on the civicrm container is...

My previously working notes were to call

```bash
env PYTHONPATH=~/dev/fundraising-dev/src/tools LOGGING=docker silverpop_export/export.py
```

The generated files will be in the \tmp directory on your docker container

### Running the sql directly

Generally you can do a lot of testing by just copying queries from update_table.sql
into your query console & running them there...

###Aliases

Note the following aliases are referenced:
alias bkb='docker-compose --file ~/dev/fundraising-dev/docker-compose.yml exec civicrm bash'
alias bkr='docker-compose --file ~/dev/fundraising-dev/docker-compose.yml exec -u root civicrm bash'

### Tox tips


List the default envs:

    tox -l

You can pass arguments to the underlying command with a double dash:

    tox -e flake8 -- --statistics
    tox -e flake8 -- ./audit
