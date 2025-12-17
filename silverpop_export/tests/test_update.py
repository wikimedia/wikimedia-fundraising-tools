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


def test_highest_donation_date(testdb):
    """
    Test that we correctly calculate the highest donation date,
    using the most recent donation date if two donations of equal amounts to endowment and foundation are the highest.
    """
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0),
        (2, 'person2@localhost', 1, 0),
        (3, 'person3@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2015-01-03', 21.25, 'xyz123', 1, 1),
        (2, 1, '2016-05-05', 21.25, 'abc456', 1, 26),
        (3, 1, '2017-05-05', 11.25, 'def789', 1, 1),
        (4, 2, '2015-02-03', 32.25, 'abc123', 1, 1),
        (5, 2, '2018-05-05', 22.25, 'xyz456', 1, 26),
        (6, 3, '2017-05-05', 13.25, 'hij789', 1, 1),
        (7, 3, '2018-03-03', 33.25, 'def123', 1, 26);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 21.25, 'USD'),
        (2, 21.25, 'USD'),
        (3, 11.25, 'USD'),
        (4, 32.25, 'USD'),
        (5, 22.25, 'USD'),
        (6, 13.25, 'USD'),
        (7, 33.25, 'USD');
    """, """
       insert into wmf_donor (entity_id, endowment_largest_donation, largest_donation) values
       (1, 21.25, 21.25),
       (2, 22.25, 32.25),
       (3, 33.25, 13.25);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select both_funds_highest_donation_date from silverpop_export_view order by ContactID")
    assert cursor.fetchone() == ('05/05/2016',)
    assert cursor.fetchone() == ('02/03/2015',)
    assert cursor.fetchone() == ('03/03/2018',)


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
    cursor.execute("select both_funds_highest_usd_amount, both_funds_highest_native_amount, both_funds_highest_native_currency from silverpop_export_view_full")
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


def test_modified_date(testdb):
    '''
    Test that we only include / exclude contacts that have been modified between 7 days and 3 minutes ago.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
    insert into civicrm_email (id, contact_id, email, is_primary, on_hold) values
        (1, 1, 'person1@localhost', 1, 0),
        (2, 2, 'person2@localhost', 1, 0),
        (3, 3, 'person3@localhost', 0, 0),
        (4, 4, 'person4@localhost', 0, 0),
        (5, 5, 'person5@localhost', 1, 0);
    """, """
    insert into log_civicrm_email (id, contact_id, email, log_date) values
        (1, 1, 'person1@localhost', DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (2, 2, 'person2@localhost', DATE_SUB(NOW(), INTERVAL 8 DAY)),
        (3, 3, 'person3@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (4, 4, 'person4@localhost', DATE_SUB(NOW(), INTERVAL 8 DAY)),
        (5, 5, 'person5@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (2, DATE_SUB(NOW(), INTERVAL 8 DAY)),
        (3, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (4, DATE_SUB(NOW(), INTERVAL 8 DAY)),
        (5, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select email from silverpop_export")
    assert cursor.fetchall() == (('person5@localhost',),)
    cursor.execute("select email from silverpop_excluded")
    assert cursor.fetchall() == (('person3@localhost',),)


def test_excluded(testdb):
    '''
    Test that we exclude all contacts that should be excluded, and only those that should be excluded.
    First test the full rebuild, where contacts are excluded no matter their modified date,
    then test the update with all the same data modified within the 7 day window.
    This covers all the excluded cases covered for update_suppression_list in test_exclusion and test_optin_negative_exclusion.
    '''
    conn, db_name = testdb
    fixture_queries = ["""
    insert into civicrm_email (id, contact_id, email, is_primary, on_hold) values
        (1, 1, 'person1@localhost', 1, 2), -- EXCL on_hold
        (2, 2, 'person2@localhost', 1, 0), -- EXCL is_opt_out
        (3, 3, 'person3@localhost', 1, 0), -- EXCL do_not_email
        (4, 4, 'person4@localhost', 1, 0), -- EXCL opt in = no
        (5, 5, 'person5@localhost', 1, 0), -- EXCL do not solicit
        (6, 6, 'person6@localhost', 1, 0), -- EXCL deleted contact
        (7, 7, 'person7@localhost', 0, 0), -- non-primary, but is primary for next, so not excluded
        (8, 8, 'person7@localhost', 1, 0),
        (9, 9, 'person9@localhost', 0, 0), -- EXCL non-primary, not primary for other
        (10, 10, 'person10@localhost', 1, 0), -- deleted but next with same email is not deleted, so not excluded
        (11, 11, 'person10@localhost', 1, 0),
        (12, 12, 'person12@localhost', 1, 0), -- deleted and opted out, but next with same email is not, so not excluded
        (13, 13, 'person12@localhost', 1, 0),
        -- 14 EXCL is only in log
        (15, 15, 'person4@localhost', 1, 0), -- also excluded because same email as #4 above
        -- 16 is only in log, not excluded as 17 below
        (17, 17, 'person17@localhost', 1, 0), -- not excluded, email previously shared with another contact
        -- 18 is only in log, not excluded as 19 below
        (19, 19, 'person19@localhost', 1, 0), -- not excluded, email previously shared with another contact
        (20, 20, 'person20@localhost', 1, 0), -- not excluded because is primary (but not modified in window)
        (21, 21, 'person20@localhost', 0, 0), -- not excluded, same email as above as non-primary, but contact modified in window
        (22, 22, 'person20@localhost', 0, 0), -- not excluded, same email as above as non-primary, but email log_date in window
        (23, 23, 'person23@localhost', 1, 0), -- primary email for next row
        (24, 24, 'person23@localhost', 0, 0), -- not excluded, same email as above as non-primary even opted out
        (99, 99, 'person99@localhost', 1, 0); -- included, making sure the highest id check doesn't affect the above
    """, """
    insert into log_civicrm_email (id, contact_id, email, log_date) values
        (1, 1, 'person1@localhost', DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (2, 2, 'person2@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, 3, 'person3@localhost', DATE_SUB(NOW(), INTERVAL 10 YEAR)),
        (4, 4, 'person4@localhost', DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (5, 5, 'person5@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (6, 6, 'person6@localhost', DATE_SUB(NOW(), INTERVAL 10 YEAR)),
        (7, 7, 'person7@localhost', DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (8, 8, 'person7@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (9, 9, 'person9@localhost', DATE_SUB(NOW(), INTERVAL 10 YEAR)),
        (10, 10, 'person10@localhost', DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (11, 11, 'person10@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (12, 12, 'person12@localhost', DATE_SUB(NOW(), INTERVAL 10 YEAR)),
        (13, 13, 'person12@localhost', DATE_SUB(NOW(), INTERVAL 1 MINUTE)),
        (14, 14, 'person14@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)), -- only in log
        (15, 15, 'person4@localhost', DATE_SUB(NOW(), INTERVAL 10 YEAR)),
        (16, 16, 'person17@localhost', DATE_SUB(NOW(), INTERVAL 100 DAY)), -- only in log for this contact, but email associated with another contact
        (17, 17, 'person17@localhost', DATE_SUB(NOW(), INTERVAL 100 DAY)),
        (18, 18, 'person19@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)), -- only in log for this contact (e.g. deleted) within the 7 day window, but email associated with another contact
        (19, 19, 'person19@localhost', DATE_SUB(NOW(), INTERVAL 100 DAY)),
        (20, 20, 'person20@localhost', DATE_SUB(NOW(), INTERVAL 100 DAY)),
        (21, 21, 'person20@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (22, 22, 'person20@localhost', DATE_SUB(NOW(), INTERVAL 100 DAY)),
        (23, 23, 'person23@localhost', DATE_SUB(NOW(), INTERVAL 100 DAY)),
        (24, 24, 'person23@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (99, 99, 'person99@localhost', DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contact (id, modified_date, is_deleted, is_opt_out, do_not_email) values
        (1, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 0, 0, 0),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 1, 0), -- is_opt_out
        (3, DATE_SUB(NOW(), INTERVAL 10 YEAR), 0, 0, 1), -- do_not_email
        (4, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 0, 0, 0),
        (5, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 0, 0),
        (6, DATE_SUB(NOW(), INTERVAL 10 YEAR), 1, 0, 0), -- deleted contact
        (7, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 0, 0, 0),
        (8, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 0, 0),
        (9, DATE_SUB(NOW(), INTERVAL 10 YEAR), 0, 0, 0),
        (10, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 1, 0, 0), -- deleted but next with same email is not deleted
        (11, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 0, 0),
        (12, DATE_SUB(NOW(), INTERVAL 10 YEAR ), 0, 0, 0),
        (13, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 0, 0, 0),
        (15, DATE_SUB(NOW(), INTERVAL 10 YEAR), 0, 0, 0),
        (16, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 0, 0), -- this contact is modified within the 7 day window, but only has the email in the log_
        (17, DATE_SUB(NOW(), INTERVAL 10 YEAR), 0, 0, 0), -- this contact has the same email as the above used to have, but not modified in the window
        (18, DATE_SUB(NOW(), INTERVAL 100 DAY), 0, 0, 0),
        (19, DATE_SUB(NOW(), INTERVAL 100 DAY), 0, 0, 0),
        (20, DATE_SUB(NOW(), INTERVAL 100 DAY), 0, 0, 0),
        (21, DATE_SUB(NOW(), INTERVAL 100 DAY), 0, 0, 0),
        (22, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 0, 0),
        (23, DATE_SUB(NOW(), INTERVAL 100 DAY), 0, 0, 0),
        (24, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 1, 0),
        (99, DATE_SUB(NOW(), INTERVAL 1 DAY), 0, 0, 0);

    """, """
    insert into civicrm_value_1_communication_4 (id, entity_id, do_not_solicit, opt_in) values
        (1, 1, 0, NULL),
        (4, 4, 0, 0), -- opt in = no
        (5, 5, 1, 0); -- do not solicit
    """]

    expected = sorted([
        ('person1@localhost',),
        ('person2@localhost',),
        ('person3@localhost',),
        ('person4@localhost',),
        ('person5@localhost',),
        ('person6@localhost',),
        ('person9@localhost',),
        ('person14@localhost',),
    ])

    run_update_with_fixtures(testdb, rebuild_suppression=1, fixture_queries=fixture_queries)

    cursor = conn.db_conn.cursor()
    cursor.execute("select email from silverpop_excluded")
    assert sorted(cursor.fetchall()) == expected

    run_update_with_fixtures(testdb, rebuild_suppression=0, fixture_queries=fixture_queries + ["""
           update log_civicrm_email
           set log_date = DATE_SUB(NOW(), INTERVAL 3 DAY)
           where id not in (16, 17, 18, 19, 20, 21, 22, 23, 24); -- these matter when they were modified
       """, """
           update civicrm_contact
           set modified_date = DATE_SUB(NOW(), INTERVAL 3 DAY)
           where id not in (16, 17, 18, 19, 20, 21, 22, 23, 24);
       """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select email from silverpop_excluded")
    assert sorted(cursor.fetchall()) == expected


def test_optin_negative_exclusion(testdb):
    '''
    Test that we include contacts for opt in = Yes or Null and exclude them for opt in = No.
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


def test_merge_status(testdb):
    '''
    Test that we correctly merge status IDs
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'neworlybunt@localhost', 1, 0),
        (2, 'neworlybunt@localhost', 1, 0),
        (3, 'newordeeplapsed@localhost', 1, 0),
        (4, 'newordeeplapsed@localhost', 1, 0),
        (5, 'newornondonor@localhost', 1, 0),
        (6, 'newornondonor@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (4, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (5, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (6, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
        (1, 1, '2025-01-01', 10, 'xyz123', 1, 1),
        (2, 2, '2024-01-01', 9, 'nnn777', 1, 1),
        (3, 3, '2025-01-01', 10, 'aaa123', 1, 1),
        (4, 4, '2018-01-01', 9, 'bbb777', 1, 1),
        (5, 5, '2025-01-01', 10, 'ccc123', 1, 1);
    """, """
    insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
        (1, 10, 'USD'),
        (2, 9, 'USD'),
        (3, 10, 'USD'),
        (4, 9, 'USD'),
        (5, 10, 'USD');
    """, """
    insert into wmf_donor (entity_id, lifetime_usd_total, last_donation_amount, last_donation_usd, last_donation_currency, first_donation_date, last_donation_date, donor_status_id) values
        (1, 10.00, 10.00, 10.00, 'USD', '2025-01-01', '2025-01-01', 25),
        (2, 9.00, 9.00, 9.00, 'USD', '2024-01-01', '2024-01-01', 35),
        (3, 10.00, 10.00, 10.00, 'USD', '2025-01-01', '2025-01-01', 25),
        (4, 9.00, 9.00, 9.00, 'USD', '2024-01-01', '2018-01-01', 70),
        (5, 10.00, 10.00, 10.00, 'USD', '2025-01-01', '2025-01-01', 25),
        (6, 0.00, 0.00, 0.00, NULL, NULL, NULL, 1000);
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select count(*) from silverpop_export")
    assert cursor.fetchone() == (3,)
    cursor.execute("select ContactID, donor_status_id, donor_status from silverpop_export_view order by ContactID")
    assert cursor.fetchone() == (1, 20, 'Consecutive')
    assert cursor.fetchone() == (3, 30, 'Active')
    assert cursor.fetchone() == (5, 25, 'New')


def test_direct_mail(testdb):
    '''
    Test that we get the most recent direct mail appeal code, if it was within the last 12 months.
    '''
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=["""
    insert into civicrm_email (contact_id, email, is_primary, on_hold) values
        (1, 'person1@localhost', 1, 0),
        (2, 'person2@localhost', 1, 0),
        (3, 'person3@localhost', 1, 0);
    """, """
    insert into civicrm_contact (id, modified_date) values
        (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (2, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
    insert into civicrm_activity_contact (activity_id, contact_id, record_type_id) values
        (1, 1, 3),
        (2, 1, 3),
        (3, 2, 3);
    """, """
    insert into civicrm_activity (id, activity_type_id, status_id, activity_date_time) values
        (1, 181, 2, DATE_SUB(NOW(), INTERVAL 11 MONTH)),
        (2, 181, 2, DATE_SUB(NOW(), INTERVAL 1 DAY)),
        (3, 181, 2, DATE_SUB(NOW(), INTERVAL 13 MONTH));
    """, """
    insert into civicrm_value_direct_mail_data (id, entity_id, direct_mail_appeal) values
        (1, 1, 'ABC'),
        (2, 2, 'DEF'),
        (3, 3, 'XYZ');
    """])

    cursor = conn.db_conn.cursor()
    cursor.execute("select direct_mail_latest_appeal from silverpop_export_view WHERE email = 'person1@localhost'")
    assert cursor.fetchone() == ('DEF',)
    cursor.execute("select direct_mail_latest_appeal from silverpop_export_view WHERE email = 'person2@localhost'")
    assert cursor.fetchone() == (None,)
    cursor.execute("select direct_mail_latest_appeal from silverpop_export_view WHERE email = 'person3@localhost'")
    assert cursor.fetchone() == (None,)


def test_opted_out_email_but_sms_consent_included(testdb):
    """
    Test that a contact with an opted-out email but a phone + phone consent
    is still included in the export.
    """
    conn, db_name = testdb

    run_update_with_fixtures(testdb, fixture_queries=[
        # Primary email for the contact
        """
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'smsconsent@localhost', 1, 0);
        """,
        # Contact modified recently so they are within the incremental window
        """
        insert into civicrm_contact (id, modified_date) values
            (1, DATE_SUB(NOW(), INTERVAL 1 DAY));
        """,
        # Email communication preference: explicitly opted out (opt_in = 0)
        """
        insert into civicrm_value_1_communication_4 (entity_id, opt_in) values
            (1, 0);
        """,
        # Phone number on the contact
        """
        insert into civicrm_phone (contact_id, phone_numeric) values
            (1, '15551234567');
        """,
        # Phone consent: opted in for SMS on that phone number
        """
        insert into civicrm_phone_consent (phone_number, opted_in) values
            ('15551234567', 1);
        """
    ])

    cursor = conn.db_conn.cursor()

    # They should still be included in the export despite the email opt-out,
    # because sms_consent = 1 causes the OR branch in the export query to pass.
    cursor.execute("select email, opted_in from silverpop_export")
    row = cursor.fetchone()
    assert row == ('smsconsent@localhost', 0)

    # And for extra safety, make sure we only exported this one contact.
    cursor.execute("select count(*) from silverpop_export")
    assert cursor.fetchone() == (1,)


def run_update_with_fixtures(testdb, fixture_path=None, fixture_queries=None, rebuild_suppression=0):
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

            if rebuild_suppression:
                update_suppression_queries = silverpop_export.update.load_queries('rebuild_suppression_list.sql')
            else:
                update_suppression_queries = silverpop_export.update.load_queries('update_suppression_list.sql')
            silverpop_export.update.run_queries(conn, update_suppression_queries)
