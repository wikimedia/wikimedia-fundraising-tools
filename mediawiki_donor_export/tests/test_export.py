import csv
import os
import tempfile
from unittest import mock

from silverpop_export.tests.test_update import testdb, run_update_with_fixtures  # noqa: F401

from mediawiki_donor_export import export


def test_export(testdb):  # noqa: F811
    """
    Smoke test: export produces a CSV with the right columns
    and correct donor status values.
    """
    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'active@localhost', 1, 0),
            (2, 'lapsed@localhost', 1, 0);
    """, """
        insert into civicrm_contact (id, modified_date) values
            (1, DATE_SUB(NOW(), INTERVAL 1 DAY)),
            (2, DATE_SUB(NOW(), INTERVAL 1 DAY));
    """, """
        insert into civicrm_value_1_communication_4 (id, entity_id, do_not_solicit) values
            (1, 1, 0),
            (2, 2, 1);
    """, """
        insert into civicrm_contribution (id, contact_id, receive_date, total_amount, trxn_id, contribution_status_id, financial_type_id) values
            (1, 1, '2025-01-01', 10.00, 'xyz1', 1, 1),
            (2, 2, '2020-01-01', 10.00, 'xyz2', 1, 1);
    """, """
        insert into wmf_contribution_extra (entity_id, original_amount, original_currency) values
            (1, 10.00, 'USD'),
            (2, 10.00, 'USD');
    """, """
        insert into wmf_donor (entity_id, donor_status_id, lifetime_usd_total, last_donation_amount, last_donation_usd, last_donation_currency, first_donation_date, last_donation_date) values
            (1, 30, 10.00, 10.00, 10.00, 'USD', '2025-01-01', '2025-01-01'),
            (2, 50, 10.00, 10.00, 10.00, 'USD', '2020-01-01', '2020-01-01');
    """])

    conn, db_name = testdb

    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch("process.globals.get_config") as mock_config:
            mock_config.return_value = mock.MagicMock(
                silverpop_db={"user": conn.connectionArgs["user"],
                              "host": conn.connectionArgs["host"],
                              "db": db_name,
                              "charset": "utf8mb4"},
                working_path=tmpdir,
            )

            output_path = export.export(days=None)

        assert os.path.exists(output_path)

        with open(output_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

    # Should have the right columns
    assert set(rows[0].keys()) == {'contact_id', 'email', 'donor_status_id', 'do_not_solicit'}

    # Both donors should be present
    emails = {row['email'] for row in rows}
    assert 'active@localhost' in emails
    assert 'lapsed@localhost' in emails

    # Check status and do_not_solicit values came through
    by_email = {row['email']: row for row in rows}
    assert by_email['active@localhost']['donor_status_id'] == '30'
    assert by_email['lapsed@localhost']['donor_status_id'] == '50'
    assert by_email['active@localhost']['do_not_solicit'] == '0'
    assert by_email['lapsed@localhost']['do_not_solicit'] == '1'
