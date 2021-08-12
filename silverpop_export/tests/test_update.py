# -*- coding: utf-8 -*-
import datetime
from decimal import Decimal
import mock
import pymysql
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

    db_params = {"user": db_user, "host": db_host, "charset": "utf8mb4"}
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
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY));
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
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("""
        select AF_has_active_recurring_donation,
            AF_recurring_latest_donation_date,
            AF_recurring_first_donation_date, AF_highest_usd_amount,
            AF_highest_native_amount, AF_highest_native_currency,
            AF_highest_donation_date, AF_lifetime_usd_total,
            AF_donation_count, AF_latest_currency, AF_latest_native_amount,
            AF_latest_donation_date
        from silverpop_export_view
    """)
    actual = cursor.fetchone()
    expected = ('No', '', '', Decimal('0.00'),
                Decimal('0.00'), '',
                '', Decimal('0.00'),
                0, '', Decimal('0.00'),
                '')
    assert actual == expected


def test_refund_history():
    '''
    Test that we don't include refunded donations in a donor's history
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2015-01-03', 15.25, 'xyz123', 1, 1),
        (2, 1, '2016-05-05', 25.25, 'abc456', 9, 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 20.15, 'CAD'),
        (2, 35.15, 'CAD');
     """, """
        insert into wmf_donor (entity_id, lifetime_usd_total, last_donation_amount, last_donation_usd, last_donation_currency, first_donation_date, last_donation_date, number_donations) values
            (1, 15.25, 20.15, 15.25, 'CAD', '2015-01-03', '2015-01-03', 1);
     """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_highest_usd_amount, lifetime_usd_total, donation_count, foundation_latest_currency, foundation_latest_native_amount, foundation_last_donation_date  from silverpop_export")
    expected = (Decimal('15.25'), Decimal('15.25'), 1, 'CAD', Decimal('20.15'), datetime.datetime(2015, 1, 3))
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
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2015-01-03', 15.25, 'xyz123', 9, 1),
        (2, 1, '2016-05-05', 25.25, 'abc456', 1, 1),
        (3, 1, '2017-05-05', 35.35, 'def789', 1, 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 20.15, 'CAD'),
        (2, 35.15, 'CAD'),
        (3, 45.25, 'CAD');
    """, """
       insert into wmf_donor (entity_id, lifetime_usd_total, last_donation_amount, last_donation_usd, last_donation_currency, first_donation_date, last_donation_date) values
       (1, 60.70, 45.25, 35.35, 'CAD', '2016-05-05', '2017-05-05');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_first_donation_date from silverpop_export")
    expected = (datetime.datetime(2016, 5, 5),)
    assert cursor.fetchone() == expected


def test_native_amount():
    '''
    Test that we correctly calculate the highest native amount and currency
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2015-01-03', 9.50, 'xyz123', 1, 1),
        (2, 1, '2016-07-07', 10.95, 'nnn777', 1, 1),
        (3, 1, '2016-05-05', 10.00, 'abc456', 1, 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 1000, 'JPY'),
        (2, 9.00, 'GBP'),
        (3, 10.00, 'USD');
        """, """
    insert into wmf_donor (entity_id, lifetime_usd_total, last_donation_amount, last_donation_usd, last_donation_currency, first_donation_date) values
         (1, 30.45, 10.00, 10.00, 'USD', '2015-01-03');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_highest_usd_amount, foundation_highest_native_amount, foundation_highest_native_currency from silverpop_export")
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
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2015-01-03', 9.50, 'xyz123', 1,1),
        (2, 1, '2017-07-07', 10.95, 'nnn777', 1, 1),
        (3, 1, '2016-05-05', 10.00, 'abc456', 1, 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 1000, 'JPY'),
        (2, 9.00, 'GBP'),
        (3, 10.00, 'USD');
            """, """
    insert into wmf_donor (entity_id, lifetime_usd_total, last_donation_amount, last_donation_usd, last_donation_currency, first_donation_date, last_donation_date) values
        (1, 30.45, 10.00, 10.00, 'GBP', '2015-01-03', '2017-07-07');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_latest_currency, foundation_latest_currency_symbol from silverpop_export")
    expected = ('GBP', u'£')
    actual = cursor.fetchone()
    assert actual == expected


def test_export_hash():
    '''
    Test that we export the contact_hash into silverpop_export.
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, hash, modified_date) values
        (1, 'abfe829234baa87s76d', DATE_SUB(NOW(), INTERVAL 1 DAY));
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
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2015-01-03', 9.50, 'xyz123', 1, 1);
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
    insert into log_civicrm_email (id, email, log_date) values
        (1, 'formerperson1@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (1, 'person1@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select email from silverpop_export")
    assert cursor.fetchone() == ('person1@localhost',)
    cursor.execute("select email from silverpop_excluded")
    assert cursor.fetchone() == ('formerperson1@localhost',)


def test_optin_negative_exclusion():
    '''
    Test that we exclude former email addresses from the log table.
    '''

    run_update_with_fixtures(fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'optinnull@localhost', 1, 0),
        (2, 'optinone@localhost', 1, 0),
        (3, 'optinzero@localhost', 1, 0);
    """, """
    insert into log_civicrm_email (id, email, log_date) values
        (1, 'optinnull@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, 'optinone@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, 'optinzero@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_value_1_communication_4 (entity_id, opt_in) values
        (2, 1),
        (3, 0);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select count(email) from silverpop_export")
    assert cursor.fetchone() == (2,)
    cursor.execute("select count(email) from silverpop_excluded")
    assert cursor.fetchone() == (1,)
    cursor.execute("select email from silverpop_excluded order by id desc")
    assert cursor.fetchone() == ('optinzero@localhost',)


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
            warnings.filterwarnings('ignore', category=pymysql.Warning)

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
            warnings.filterwarnings('default', category=pymysql.Warning)

            drop_queries = silverpop_export.update.load_queries('drop_schema.sql')
            silverpop_export.update.run_queries(conn, drop_queries)

            drop_incremental_queries = silverpop_export.update.load_queries('drop_incremental_schema.sql')
            silverpop_export.update.run_queries(conn, drop_incremental_queries)

            rebuild_queries = silverpop_export.update.load_queries('rebuild_schema.sql')
            silverpop_export.update.run_queries(conn, rebuild_queries)

            staging_update_queries = silverpop_export.update.load_queries('update_silverpop_staging.sql')
            silverpop_export.update.run_queries(conn, staging_update_queries)

            update_queries = silverpop_export.update.load_queries('update_table.sql')
            silverpop_export.update.run_queries(conn, update_queries)

            update_suppression_queries = silverpop_export.update.load_queries('update_suppression_list.sql')
            silverpop_export.update.run_queries(conn, update_suppression_queries)
