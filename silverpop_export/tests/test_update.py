# -*- coding: utf-8 -*-
import datetime
from decimal import Decimal
from unittest import mock
import pymysql
import os
import warnings

import database.db
import silverpop_export.update

import pytest


@pytest.fixture()
def testdb():
    # FIXME: parameterize test configuration better
    db_pass = None
    if 'CI' in os.environ:
        # We're running under CI.  Assume things.
        db_name = "test"
        db_user = "root"
        db_host = "127.0.0.1"
    else:
        db_name = "test"
        db_user = "test"
        db_host = "database"

    db_params = {"user": db_user, "host": db_host, "charset": "utf8mb4"}
    if db_pass:
        db_params['passwd'] = db_pass

    conn = database.db.Connection(**db_params)
    conn.execute("set default_storage_engine=memory")
    conn.execute("drop database if exists " + db_name)
    conn.execute("create database " + db_name)
    conn.db_conn.select_db(db_name)

    return [conn, db_name]


def test_test_setup(testdb):
    '''
    Set up the civcrm and export databases and run the update with no data.
    '''
    conn, db_name = testdb
    print("Conn:", conn, "dbname:", db_name)

    run_update_with_fixtures(testdb, fixture_queries=[])


def test_duplicate(testdb):
    '''
    Test that we export one record for a duplicate contact.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_tag(testdb):
    '''
    Test that we export preference tags.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0),
        (2, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_entity_tag (id, tag_id, entity_id) values
        (1, 1, 1),
        (2, 2, 1),
        (3, 2, 2);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select preferences_tags from silverpop_export_view WHERE email = 'person1@localhost'")
    assert cursor.fetchone() == ('exclude-from-6C-annual-campaigns;exclude-from-direct-mail-campaigns',)


def test_no_donations(testdb):
    '''
    Test that we set the donation-related fields correctly when a contact has
    no donations.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_refund_history(testdb):
    '''
    Test that we don't include refunded donations in a donor's history
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_first_donation(testdb):
    """
    Test that we correctly calculate the first donation date,
    not counting refunded donations.
    """
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_native_amount(testdb):
    '''
    Test that we correctly calculate the highest native amount and currency
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_currency_symbol(testdb):
    '''
    Test that we correctly pull in the currency symbol for the latest donation
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_export_hash(testdb):
    '''
    Test that we export the contact_hash into silverpop_export.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, hash, modified_date) values
        (1, 'abfe829234baa87s76d', DATE_SUB(NOW(), INTERVAL 1 DAY));
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select contact_hash from silverpop_export")
    assert cursor.fetchone() == ('abfe829234baa87s76d',)


def test_bad_ct_country(testdb):
    '''
    Test that we use the Civi address in place of XX civicrm_contribution_tracking
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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
    insert into civicrm_contribution_tracking (contribution_id, country) values
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


def test_exclusion(testdb):
    '''
    Test that we exclude former email addresses from the log table.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_optin_negative_exclusion(testdb):
    '''
    Test that we exclude former email addresses from the log table.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
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


def test_employer_id_filter(testdb):
    '''
    Test that we only export employer ID and name when provided_by_donor is true
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_relationship_type (id, name_a_b) values
            (4, 'Sibling of'),
            (5, 'Employee of');
        """, """
        insert into civicrm_contact (id, contact_type) values
            (1, 'Organization');
        """, """
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (2, 'nocustomfield@localhost', 1, 0),
            (3, 'customfieldone@localhost', 1, 0),
            (4, 'customfieldzero@localhost', 1, 0),
            (5, 'wrongrelationshiptype@localhost', 1, 0);
        """, """
        insert into civicrm_contact (id, employer_id, organization_name, modified_date) values
            (2, 1, 'Blah de Blah', DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (3, 1, 'Blah de Blah', DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (4, 1, 'Blah de Blah', DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (5, 1, 'Blah de Blah', DATE_SUB(NOW(), INTERVAL 1 DAY));
        """, """
        insert into civicrm_relationship (id, contact_id_a, contact_id_b, relationship_type_id, is_active) values
            (1, 2, 1, 5, 1),
            (2, 3, 1, 5, 1),
            (3, 4, 1, 5, 1),
            (4, 5, 1, 4, 1);
        """, """
        insert into civicrm_value_relationship_metadata (entity_id, provided_by_donor) values
            (2, 1),
            (3, 0),
            (4, 1);
        """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select count(employer_id) from silverpop_export")
    assert cursor.fetchone() == (1,)
    cursor.execute("select employer_id, employer_name from silverpop_export where email='customfieldone@localhost'")
    assert cursor.fetchone() == (1, 'Blah de Blah',)


def test_multiple_recurring(testdb):
    """
    Test that we correctly calculate the number of active recurrings and the latest ID
    """
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'person1@localhost', 1, 0);
        """, """
        insert into civicrm_contact (id, modified_date) values
            (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
        """, """
        insert into civicrm_contribution_recur (id, contact_id, amount, currency, contribution_status_id, cancel_date) values
            (1, 1, 1.01, 'USD', 5, NULL),
            (3, 1, 2.02, 'EUR', 5, NULL),
            (5, 1, 3.03, 'GBP', 3, '2023-05-11');
        """, """
        insert into civicrm_contribution (id, contact_id, contribution_recur_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
            (1, 1, 1, '2015-01-03', 1.01, 'xyz123', 1, 1),
            (2, 1, 3, '2016-05-05', 2.02, 'abc456', 1, 1),
            (3, 1, 3, '2017-05-05', 2.02, 'def789', 1, 1),
            (4, 1, 5, '2017-05-05', 3.03, 'ghi012', 1, 1),
            (5, 1, 5, '2017-05-05', 3.03, 'jkl345', 9, 1);
        """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_recurring_active_count, foundation_recurring_latest_contribution_recur_id from silverpop_export")
    assert cursor.fetchone() == (2, 3,)


def test_multiple_only_inactive_recurring(testdb):
    """
    Test that we correctly calculate the number of inactive recurrings and the latest ID
    """
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'person1@localhost', 1, 0);
        """, """
        insert into civicrm_contact (id, modified_date) values
            (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
        """, """
        insert into civicrm_contribution_recur (id, contact_id, amount, currency, contribution_status_id, cancel_date) values
            (1, 1, 1.01, 'USD', 5, '2023-05-11'),
            (3, 1, 2.02, 'EUR', 5, '2023-05-11'),
            (5, 1, 3.03, 'GBP', 3, '2023-05-11');
        """, """
        insert into civicrm_contribution (id, contact_id, contribution_recur_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
            (1, 1, 1, '2015-01-03', 1.01, 'xyz123', 1, 1),
            (2, 1, 3, '2016-05-05', 2.02, 'abc456', 1, 1),
            (3, 1, 3, '2017-05-05', 2.02, 'def789', 1, 1),
            (4, 1, 5, '2017-05-05', 3.03, 'ghi012', 1, 1),
            (5, 1, 5, '2017-05-05', 3.03, 'jkl345', 9, 1);
        """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_recurring_active_count, foundation_recurring_latest_contribution_recur_id from silverpop_export")
    assert cursor.fetchone() == (0, 5,)


def test_recurring_upgrade_eligibility(testdb):
    """
    Test that we correctly calculate who is eligible for a recurring upgrade solicitation.
    PayPal donors are not, nor are donors with multiple recurrings or any upgrade activities.
    """
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_payment_processor (id, name) values
            (1, 'adyen'),
            (2, 'paypal');
        """, """
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'paypaldonor@localhost', 1, 0),
            (2, 'adyendonor@localhost', 1, 0),
            (3, 'multipledonor@localhost', 1, 0),
            (4, 'alreadyupgraded@localhost', 1, 0),
            (5, 'annualdonor@localhost', 1, 0);
        """, """
        insert into civicrm_contact (id, modified_date) values
            (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (2, DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (3, DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (4, DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (5, DATE_SUB(NOW(), INTERVAL 1 DAY));
        """, """
        insert into civicrm_contribution_recur (id, contact_id, amount, currency, contribution_status_id, frequency_unit, payment_processor_id ) values
            (1, 1, 1.01, 'USD', 5, 'month', 2),
            (2, 2, 2.02, 'EUR', 5, 'month', 1),
            (3, 3, 3.03, 'GBP', 5, 'month', 1),
            (4, 3, 4.04, 'PLN', 5, 'month', 1),
            (5, 4, 5.05, 'COP', 5, 'month', 1),
            (6, 5, 6.06, 'COP', 5, 'year', 1);
        """, """
        insert into civicrm_contribution (id, contact_id, contribution_recur_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
            (1, 1, 1, '2015-01-03', 1.01, 'xyz123', 1, 1),
            (2, 2, 2, '2016-05-05', 2.02, 'abc456', 1, 1),
            (3, 3, 3, '2017-05-05', 3.03, 'def789', 1, 1),
            (4, 3, 4, '2017-05-05', 4.04, 'ghi012', 1, 1),
            (5, 4, 5, '2017-05-05', 5.05, 'jkl345', 1, 1),
            (6, 5, 6, '2017-05-05', 6.06, 'mno678', 1, 1);
        """, """
        insert into civicrm_activity (id, activity_type_id) values
            (1, 165);
        """, """
        insert into civicrm_activity_contact (activity_id, contact_id) values
            (1, 4);
        """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select foundation_recurring_active_count, recurring_has_upgrade_activity from silverpop_export where contact_id=2")
    assert cursor.fetchone(), (1, 0,)
    cursor.execute("select ContactID, AF_recurring_eligible_for_upgrade from silverpop_export_view order by ContactID")
    assert cursor.fetchone() == (1, 'No',)
    assert cursor.fetchone() == (2, 'Yes',)
    assert cursor.fetchone() == (3, 'No',)
    assert cursor.fetchone() == (4, 'No',)
    assert cursor.fetchone() == (5, 'No',)


def run_update_with_fixtures(testdb, fixture_path=None, fixture_queries=None):
    conn, db_name = testdb

    with mock.patch("database.db.Connection") as MockConnection:

        # Always return our test database connection.
        MockConnection.return_value = conn

        with mock.patch("process.globals.get_config") as MockConfig:
            # Point all config at our test database.
            MockConfig().civicrm_db.db = db_name
            MockConfig().drupal_db.db = db_name
            MockConfig().silverpop_db.db = db_name
            MockConfig().log_civicrm_db.db = db_name
            MockConfig().offset_in_days = 7

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
