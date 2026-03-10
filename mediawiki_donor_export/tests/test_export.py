import csv
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta
from unittest import mock

import pytest

from silverpop_export.tests.test_update import testdb, run_update_with_fixtures  # noqa: F401
from process.globals import DictAsAttrDict

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
        with mock.patch("process.globals.get_config") as mock_config, \
             mock.patch("mediawiki_donor_export.export.check_data_freshness"):
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


def test_fresh_data_permits_export():
    """Data updated 1 hour ago should pass the freshness check."""
    db = _make_db(datetime.now() - timedelta(hours=1))
    export.check_data_freshness(db, max_staleness_hours=36)


def test_stale_data_blocks_export():
    """Data updated 48 hours ago should block the export."""
    db = _make_db(datetime.now() - timedelta(hours=48))
    with pytest.raises(RuntimeError, match="stale"):
        export.check_data_freshness(db, max_staleness_hours=36)


def test_null_update_time_blocks_export():
    """NULL UPDATE_TIME (table missing or unsupported engine) should block."""
    db = _make_db(None)
    with pytest.raises(RuntimeError, match="Cannot determine"):
        export.check_data_freshness(db, max_staleness_hours=36)


def test_no_rows_blocks_export():
    """No rows from information_schema (table doesn't exist) should block."""
    db = mock.MagicMock()
    db.execute.return_value = iter([])
    with pytest.raises(RuntimeError, match="Cannot determine"):
        export.check_data_freshness(db, max_staleness_hours=36)


def test_one_second_past_threshold_blocks_export():
    """Data one second beyond the threshold should block."""
    db = _make_db(datetime.now() - timedelta(hours=36, seconds=1))
    with pytest.raises(RuntimeError, match="stale"):
        export.check_data_freshness(db, max_staleness_hours=36)


def test_encrypt_file_calls_openssl_correctly():
    """encrypt_file invokes openssl with key in env, not argv."""
    key = 'my-test-secret'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    with mock.patch("mediawiki_donor_export.export.subprocess.run") as mock_run, \
         mock.patch("mediawiki_donor_export.export.os.remove") as mock_remove:
        enc_path = export.encrypt_file(plaintext_path, key)

    assert enc_path == plaintext_path + '.enc'
    call_args = mock_run.call_args
    assert call_args[0][0] == [
        'openssl', 'enc', '-aes-256-cbc', '-salt', '-pbkdf2',
        '-in', plaintext_path,
        '-out', enc_path,
        '-pass', 'env:OPENSSL_PASS',
    ]
    assert call_args[1]['check'] is True
    assert call_args[1]['env']['OPENSSL_PASS'] == key
    assert key not in call_args[0][0]
    mock_remove.assert_called_once_with(plaintext_path)
    os.unlink(plaintext_path)


def test_encrypt_file_keeps_plaintext_on_failure():
    """encrypt_file does not remove plaintext when openssl fails."""
    key = 'my-test-secret'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    with mock.patch("mediawiki_donor_export.export.subprocess.run",
                    side_effect=subprocess.CalledProcessError(1, 'openssl')):
        with pytest.raises(subprocess.CalledProcessError):
            export.encrypt_file(plaintext_path, key)

    assert os.path.exists(plaintext_path)
    os.unlink(plaintext_path)


def test_export_with_encryption():
    """Export with encryption_key calls encrypt_file."""
    key = 'my-export-secret'
    fake_rows = [
        {'contact_id': 1, 'email': 'enc@localhost', 'donor_status_id': 30, 'do_not_solicit': 0}
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch("process.globals.get_config") as mock_config, \
             mock.patch("mediawiki_donor_export.export.check_data_freshness"), \
             mock.patch("mediawiki_donor_export.export.DbConnection") as mock_db_cls, \
             mock.patch("mediawiki_donor_export.export.encrypt_file") as mock_encrypt:
            mock_db_cls.return_value.execute.return_value = iter(fake_rows)
            mock_encrypt.side_effect = lambda path, k: path + '.enc'
            mock_config.return_value = DictAsAttrDict(
                silverpop_db={},
                working_path=tmpdir,
                encryption_key=key,
            )

            output_path = export.export(days=None)

        assert output_path.endswith('.enc')
        mock_encrypt.assert_called_once()
        assert mock_encrypt.call_args[0][1] == key


@pytest.mark.skipif(not shutil.which('openssl'), reason='openssl not on PATH')
def test_encrypt_file_roundtrip_integration():
    """Integration: encrypt then decrypt with openssl, verify contents."""
    key = 'integration-test-key'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    enc_path = export.encrypt_file(plaintext_path, key)

    env = os.environ.copy()
    env['OPENSSL_PASS'] = key
    result = subprocess.run(
        ['openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2',
         '-in', enc_path, '-pass', 'env:OPENSSL_PASS'],
        capture_output=True,
        env=env,
    )
    assert result.returncode == 0
    assert result.stdout == b'hello,world\n'
    os.unlink(enc_path)


def test_export_without_encryption_key():
    """Backwards compat: no encryption_key means plain CSV output."""
    fake_rows = [
        {'contact_id': 1, 'email': 'plain@localhost', 'donor_status_id': 30, 'do_not_solicit': 0}
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch("process.globals.get_config") as mock_config, \
             mock.patch("mediawiki_donor_export.export.check_data_freshness"), \
             mock.patch("mediawiki_donor_export.export.DbConnection") as mock_db_cls:
            mock_db_cls.return_value.execute.return_value = iter(fake_rows)
            mock_config.return_value = DictAsAttrDict(
                silverpop_db={},
                working_path=tmpdir,
            )

            output_path = export.export(days=None)

        assert output_path.endswith('.csv')
        assert os.path.exists(output_path)

        with open(output_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert any(r['email'] == 'plain@localhost' for r in rows)


def _make_db(update_time):
    """Create a mock db that returns the given UPDATE_TIME."""
    db = mock.MagicMock()
    if update_time is None:
        db.execute.return_value = iter([{'UPDATE_TIME': None}])
    else:
        db.execute.return_value = iter([{'UPDATE_TIME': update_time}])
    return db
