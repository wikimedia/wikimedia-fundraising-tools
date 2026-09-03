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
    Smoke test: export produces a CSV with the right columns, and
    donor_status_otg / donor_status_recur_overall map to the expected
    relationship_type, including CASE precedence when a donor matches
    more than one condition.
    """
    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'sustaining@localhost', 1, 0),
            (2, 'returning@localhost', 1, 0),
            (3, 'new@localhost', 1, 0),
            (4, 'lapsed-recur@localhost', 1, 0),
            (5, 'lapsed-otg@localhost', 1, 0),
            (6, 'reader@localhost', 1, 0);
    """, """
        insert into civicrm_contact (id, modified_date)
        select distinct contact_id, DATE_SUB(NOW(), INTERVAL 1 DAY)
        from civicrm_email;
    """, """
        insert into civicrm_value_1_communication_4 (id, entity_id, do_not_solicit)
        select id, id, 0
        from civicrm_contact;
    """, """
        insert into wmf_donor (entity_id, donor_status_otg, donor_status_recur_overall) values
            (1, 10, 15),  -- recur_overall -> Sustaining donor (precedence over otg=10 "Returning")
            (2, 10, 55),  -- otg -> Returning / Loyal Supporter
            (3, 30, 65),  -- otg -> New / Recent Supporter
            (4, 70, 55),  -- recur_overall -> Lapsed Supporter
            (5, 70, 95),  -- otg -> Lapsed Supporter
            (6, 99, 95);  -- neither -> Contactable Reader
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
            reader = csv.DictReader(f, fieldnames=['email', 'relationship_type'])
            rows = list(reader)

    # All donors should be present with the expected relationship_type
    expected = {
        'sustaining@localhost': '2',
        'returning@localhost': '4',
        'new@localhost': '1',
        'lapsed-recur@localhost': '3',
        'lapsed-otg@localhost': '3',
        'reader@localhost': '5',
    }
    by_email = {row['email']: row for row in rows}
    for email, relationship_type in expected.items():
        assert by_email[email]['relationship_type'] == relationship_type


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


def test_encrypt_file_calls_age_correctly():
    """encrypt_file invokes age with identity file path."""
    identity_path = '/path/to/identity.txt'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    with mock.patch("mediawiki_donor_export.export.subprocess.run") as mock_run, \
         mock.patch("mediawiki_donor_export.export.os.remove") as mock_remove:
        enc_path = export.encrypt_file(plaintext_path, identity_path)

    assert enc_path == plaintext_path + '.age'
    call_args = mock_run.call_args
    assert call_args[0][0] == [
        'age',
        '-e',
        '-i', identity_path,
        '-o', enc_path,
        plaintext_path
    ]
    assert call_args[1]['check'] is True
    mock_remove.assert_called_once_with(plaintext_path)
    os.unlink(plaintext_path)


def test_encrypt_file_keeps_plaintext_on_failure():
    """encrypt_file does not remove plaintext when age fails."""
    identity_path = '/path/to/identity.txt'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    with mock.patch("mediawiki_donor_export.export.subprocess.run",
                    side_effect=subprocess.CalledProcessError(1, 'age')):
        with pytest.raises(subprocess.CalledProcessError):
            export.encrypt_file(plaintext_path, identity_path)

    assert os.path.exists(plaintext_path)
    os.unlink(plaintext_path)


def test_export_with_encryption():
    """Export with age_identity_file calls encrypt_file."""
    identity_path = '/path/to/identity.txt'
    fake_rows = [
        {'email': 'enc@localhost', 'relationship_type': 2}
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch("process.globals.get_config") as mock_config, \
             mock.patch("mediawiki_donor_export.export.check_data_freshness"), \
             mock.patch("mediawiki_donor_export.export.DbConnection") as mock_db_cls, \
             mock.patch("mediawiki_donor_export.export.encrypt_file") as mock_encrypt:
            mock_db_cls.return_value.execute.return_value = iter(fake_rows)
            mock_encrypt.side_effect = lambda path, k: path + '.age'
            mock_config.return_value = DictAsAttrDict(
                silverpop_db={},
                working_path=tmpdir,
                age_identity_file=identity_path,
            )

            output_path = export.export(days=None)

        assert output_path.endswith('.age')
        mock_encrypt.assert_called_once()
        assert mock_encrypt.call_args[0][1] == identity_path


@pytest.mark.skipif(not shutil.which('age'), reason='age not on PATH')
def test_encrypt_file_roundtrip_integration():
    """Integration: encrypt then decrypt with age, verify contents."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        identity_path = f.name
    os.unlink(identity_path)

    subprocess.run(['age-keygen', '-o', identity_path],
                   check=True, capture_output=True)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    enc_path = export.encrypt_file(plaintext_path, identity_path)

    result = subprocess.run(
        ['age', '-d', '-i', identity_path, enc_path],
        capture_output=True,
    )

    assert result.returncode == 0
    assert result.stdout == b'hello,world\n'
    os.unlink(enc_path)
    os.unlink(identity_path)


def test_export_without_encryption_key():
    """Backwards compat: no encryption_key means plain CSV output."""
    fake_rows = [
        {'email': 'plain@localhost', 'relationship_type': 2}
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
            reader = csv.DictReader(f, fieldnames=['email', 'relationship_type'])
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
