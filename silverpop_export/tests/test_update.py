# -*- coding: utf-8 -*-
import datetime
from decimal import Decimal
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

    db_params = {"user": db_user, "host": db_host, "charset": "utf8"}
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


def test_no_donations():
    '''
    Test that we set the donation-related fields correctly when a contact has
    no donations.
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("""
        select has_recurred_donation, highest_usd_amount,
            highest_native_amount, highest_native_currency,
            highest_donation_date, lifetime_usd_total,
            donation_count, latest_currency, latest_native_amount,
            latest_usd_amount, latest_donation_date
        from silverpop_export_view
    """)
    actual = cursor.fetchone()
    expected = ('NO', Decimal('0.00'),
                Decimal('0.00'), '',
                '', Decimal('0.00'),
                0, '', Decimal('0.00'),
                Decimal('0.00'), '')
    assert actual == expected


def test_refund_history():
    '''
    Test that we don't include refunded donations in a donor's history
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1);
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id) values
        (1, 1, '2015-01-03', 15.25, 'xyz123', 1),
        (2, 1, '2016-05-05', 25.25, 'abc456', 9);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 20.15, 'CAD'),
        (2, 35.15, 'CAD');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select highest_usd_amount, lifetime_usd_total, donation_count, latest_currency, latest_native_amount, latest_usd_amount, latest_donation  from silverpop_export")
    expected = (Decimal('15.25'), Decimal('15.25'), 1, 'CAD', Decimal('20.15'), Decimal('15.25'), datetime.datetime(2015, 1, 3))
    assert cursor.fetchone() == expected


def test_first_donation():
    """
    Test that we correctly calculate the first donation date,
    not counting refunded donations.
    """

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1);
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id) values
        (1, 1, '2015-01-03', 15.25, 'xyz123', 9),
        (2, 1, '2016-05-05', 25.25, 'abc456', 1),
        (3, 1, '2017-05-05', 35.35, 'def789', 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 20.15, 'CAD'),
        (2, 35.15, 'CAD'),
        (3, 45.25, 'CAD');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select first_donation_date from silverpop_export")
    expected = (datetime.datetime(2016, 5, 5),)
    assert cursor.fetchone() == expected


def test_timezone():
    '''
    Test that we export timezone records where they exist
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0),
        (2, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1),
        (2);
    """, """
    insert into civicrm_country (id, iso_code) values
        (1, 'US');
    """, """
    insert into civicrm_address (contact_id, is_primary, country_id, postal_code, timezone) values
        (1, 1, 1, '10027', 'UTC-5');
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id) values
        (1, 1, '2015-01-03', 15.25, 'xyz123', 1),
        (2, 1, '2016-05-05', 25.25, 'abc456', 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 20.15, 'USD'),
        (2, 35.15, 'USD');
    """, """
    insert into contribution_tracking (contribution_id, country) values
        (1, 'US'),
        (2, 'US');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select email, country, postal_code, timezone from silverpop_export")
    expected = ('person1@localhost', 'US', '10027', 'UTC-5')
    assert cursor.fetchone() == expected


def test_native_amount():
    '''
    Test that we correctly calculate the highest native amount and currency
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1);
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id) values
        (1, 1, '2015-01-03', 9.50, 'xyz123', 1),
        (2, 1, '2016-07-07', 10.95, 'nnn777', 1),
        (3, 1, '2016-05-05', 10.00, 'abc456', 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 1000, 'JPY'),
        (2, 9.00, 'GBP'),
        (3, 10.00, 'USD');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select highest_usd_amount, highest_native_amount, highest_native_currency from silverpop_export")
    expected = (Decimal('10.95'), Decimal('9'), 'GBP')
    actual = cursor.fetchone()
    assert actual == expected


def test_currency_symbol():
    '''
    Test that we correctly pull in the currency symbol for the latest donation
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1);
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id) values
        (1, 1, '2015-01-03', 9.50, 'xyz123', 1),
        (2, 1, '2017-07-07', 10.95, 'nnn777', 1),
        (3, 1, '2016-05-05', 10.00, 'abc456', 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 1000, 'JPY'),
        (2, 9.00, 'GBP'),
        (3, 10.00, 'USD');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select latest_currency, latest_currency_symbol from silverpop_export")
    expected = ('GBP', u'£')
    actual = cursor.fetchone()
    assert actual == expected


def test_export_hash():
    '''
    Test that we export one record for a duplicate contact.
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, hash) values
        (1, 'abfe829234baa87s76d');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select contact_hash from silverpop_export")
    assert cursor.fetchone() == ('abfe829234baa87s76d',)


def test_bad_ct_country():
    '''
    Test that we use the Civi address in place of XX contribution_tracking
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id) values
        (1);
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id) values
        (1, 1, '2015-01-03', 9.50, 'xyz123', 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 1000, 'JPY');
    """, """
    insert into contribution_tracking (contribution_id, country) values
        (1, 'XX');
    """, """
    insert into civicrm_country (id, iso_code) values
        (1, 'PE');
    """, """
    insert into civicrm_address (contact_id, is_primary, country_id) values
        (1, 1, 1);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select country from silverpop_export")
    assert cursor.fetchone() == ('PE',)


def test_exclusion():
    '''
    Test that we exclude former email addresses from the log table.
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into log_civicrm_email (id, email) values
        (1, 'formerperson1@localhost'),
        (1, 'person1@localhost');
    """, """
    insert into civicrm_contact (id) values
        (1);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select email from silverpop_export")
    assert cursor.fetchone() == ('person1@localhost',)
    cursor.execute("select email from silverpop_excluded")
    assert cursor.fetchone() == ('formerperson1@localhost',)


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
