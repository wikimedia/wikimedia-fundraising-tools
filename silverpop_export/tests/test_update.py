import mock
import MySQLdb
import os
import warnings

import database.db
import silverpop_export.update

conn = None
db_name = None


def setup():
    global conn
    global db_name
    # FIXME: parameterize test configuration better
    db_host = "127.0.0.1"
    db_pass = None
    if 'EXECUTOR_NUMBER' in os.environ:
        # We're running under Jenkins.  Assume things.
        db_name = "test"
        db_user = "root"
    else:
        db_name = "test"
        db_user = "test"

    db_params = {"user": db_user, "host": db_host}
    if db_pass:
        db_params['passwd'] = db_pass

    conn = database.db.Connection(**db_params)
    conn.execute("set default_storage_engine=memory")
    conn.execute("drop database if exists " + db_name)
    conn.execute("create database " + db_name)
    conn.db_conn.select_db(db_name)


def test_test_setup():
    '''
    Set up the civcrm and export databases and run the update with no data.
    '''
    run_update_with_fixtures(fixture_queries=[])


def test_duplicate():
    '''
    Test that we export one record for a duplicate contact.
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0),
        (2, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1),
        (2);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select count(*) from silverpop_export")
    assert cursor.fetchone() == (1,)


def run_update_with_fixtures(fixture_path=None, fixture_queries=None):
    with mock.patch("database.db.Connection") as MockConnection:

        # Always return our test database connection.
        MockConnection.return_value = conn

        with mock.patch("process.globals.get_config") as MockConfig:
            # Point all config at our test database.
            MockConfig().civicrm_db.db = db_name
            MockConfig().drupal_db.db = db_name
            MockConfig().silverpop_db.db = db_name
            MockConfig().log_civicrm_db.db = db_name

            # Silence predictable warnings about "if not exists" table stuff.
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)

            # Create fixtures
            tests_dir = os.path.dirname(__file__)
            script_path = os.path.join(tests_dir, "minimal_schema.sql")
            database.db.run_script(script_path)

            parent_dir = os.path.dirname(os.path.dirname(__file__))
            script_path = os.path.join(parent_dir, "silverpop_countrylangs.sql")
            database.db.run_script(script_path)

            if fixture_path:
                database.db.run_script(fixture_path)

            if fixture_queries:
                for statement in fixture_queries:
                    conn.execute(statement)

            # Reenable warnings
            warnings.filterwarnings('default', category=MySQLdb.Warning)

            # Run the bulk update.
            # FIXME: Implementation should provide this as a single function.
            update_queries = silverpop_export.update.load_queries('update_table.sql')
            silverpop_export.update.run_queries(conn, update_queries)
