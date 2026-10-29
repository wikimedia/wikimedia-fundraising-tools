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
    Smoke test: export produces a CSV with no header row, and each
    donor_status_otg / donor_status_recur_overall value gives the expected
    relationship_type. When a donor matches more than one rule, the rule
    that comes first in the export query is used.
    """
    run_update_with_fixtures(testdb, fixture_queries=["""
        insert into civicrm_email (contact_id, email, is_primary, on_hold) values
            (1, 'recur-active@localhost', 1, 0),
            (2, 'recur-new@localhost', 1, 0),
            (3, 'recur-paused@localhost', 1, 0),
            (4, 'recur-failing@localhost', 1, 0),
            (5, 'recur-active-and-otg-consecutive@localhost', 1, 0),
            (6, 'otg-consecutive@localhost', 1, 0),
            (7, 'otg-reactivated@localhost', 1, 0),
            (8, 'otg-consecutive-last-year@localhost', 1, 0),
            (9, 'otg-reactivated-last-year@localhost', 1, 0),
            (10, 'otg-consecutive-and-recur-failed@localhost', 1, 0),
            (11, 'otg-new@localhost', 1, 0),
            (12, 'otg-new-last-year@localhost', 1, 0),
            (13, 'otg-new-and-recur-cancelled@localhost', 1, 0),
            (14, 'recur-failed@localhost', 1, 0),
            (15, 'recur-cancelled@localhost', 1, 0),
            (16, 'otg-lapsed@localhost', 1, 0),
            (17, 'otg-deep-lapsed@localhost', 1, 0),
            (18, 'otg-ultra-lapsed@localhost', 1, 0),
            (19, 'otg-lapsed-and-recur-failed@localhost', 1, 0),
            (20, 'reader@localhost', 1, 0);
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
            -- Sustaining Donor (2): recur in 15, 25, 35, 45
            (1, 99, 15),
            (2, 99, 25),
            (3, 99, 35),
            (4, 99, 45),
            (5, 10, 15),   -- also matches otg 10 (Returning), but the recur rule is checked first
            -- Returning / Loyal Supporter (4): otg in 10, 20, 40, 50
            (6, 10, 95),
            (7, 20, 95),
            (8, 40, 95),
            (9, 50, 95),
            (10, 10, 55),  -- also matches recur 55 (Lapsed), but the otg rule is checked first
            -- New / Recent Supporter (1): otg in 30, 60
            (11, 30, 95),
            (12, 60, 95),
            (13, 30, 65),  -- also matches recur 65 (Lapsed), but the otg rule is checked first
            -- Lapsed Supporter (3): recur in 55, 65, or otg in 70, 80, 90
            (14, 99, 55),
            (15, 99, 65),
            (16, 70, 95),
            (17, 80, 95),
            (18, 90, 95),
            (19, 70, 55),
            -- Contactable Reader (5): no rule matches
            (20, 99, 95);
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

    expected = {
        'recur-active@localhost': '2',
        'recur-new@localhost': '2',
        'recur-paused@localhost': '2',
        'recur-failing@localhost': '2',
        'recur-active-and-otg-consecutive@localhost': '2',
        'otg-consecutive@localhost': '4',
        'otg-reactivated@localhost': '4',
        'otg-consecutive-last-year@localhost': '4',
        'otg-reactivated-last-year@localhost': '4',
        'otg-consecutive-and-recur-failed@localhost': '4',
        'otg-new@localhost': '1',
        'otg-new-last-year@localhost': '1',
        'otg-new-and-recur-cancelled@localhost': '1',
        'recur-failed@localhost': '3',
        'recur-cancelled@localhost': '3',
        'otg-lapsed@localhost': '3',
        'otg-deep-lapsed@localhost': '3',
        'otg-ultra-lapsed@localhost': '3',
        'otg-lapsed-and-recur-failed@localhost': '3',
        'reader@localhost': '5',
    }
    actual = {row['email']: row['relationship_type'] for row in rows}

    # One row per donor, with no header row
    assert len(rows) == len(expected)
    assert actual == expected


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
    """encrypt_file invokes age with the recipient public key."""
    recipient = 'age1examplepublickey'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    with mock.patch("mediawiki_donor_export.export.subprocess.run") as mock_run, \
         mock.patch("mediawiki_donor_export.export.os.remove") as mock_remove:
        enc_path = export.encrypt_file(plaintext_path, recipient)

    assert enc_path == plaintext_path + '.age'
    call_args = mock_run.call_args
    assert call_args[0][0] == [
        'age',
        '-e',
        '-r', recipient,
        '-o', enc_path,
        plaintext_path
    ]
    assert call_args[1]['check'] is True
    mock_remove.assert_called_once_with(plaintext_path)
    os.unlink(plaintext_path)


def test_encrypt_file_keeps_plaintext_on_failure():
    """encrypt_file does not remove plaintext when age fails."""
    recipient = 'age1examplepublickey'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    with mock.patch("mediawiki_donor_export.export.subprocess.run",
                    side_effect=subprocess.CalledProcessError(1, 'age')):
        with pytest.raises(subprocess.CalledProcessError):
            export.encrypt_file(plaintext_path, recipient)

    assert os.path.exists(plaintext_path)
    os.unlink(plaintext_path)


def test_export_with_encryption():
    """Export with age_recipient calls encrypt_file."""
    recipient = 'age1examplepublickey'
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
                age_recipient=recipient,
            )

            output_path = export.export(days=None)

        assert output_path.endswith('.age')
        mock_encrypt.assert_called_once()
        assert mock_encrypt.call_args[0][1] == recipient


@pytest.mark.skipif(not shutil.which('age'), reason='age not on PATH')
def test_encrypt_file_roundtrip_integration():
    """Integration: encrypt then decrypt with age, verify contents."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        identity_path = f.name
    os.unlink(identity_path)

    subprocess.run(['age-keygen', '-o', identity_path],
                   check=True, capture_output=True)
    recipient = subprocess.run(['age-keygen', '-y', identity_path],
                               check=True, capture_output=True,
                               text=True).stdout.strip()

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('hello,world\n')
        plaintext_path = f.name

    enc_path = export.encrypt_file(plaintext_path, recipient)

    result = subprocess.run(
        ['age', '-d', '-i', identity_path, enc_path],
        capture_output=True,
    )

    assert result.returncode == 0
    assert result.stdout == b'hello,world\n'
    os.unlink(enc_path)
    os.unlink(identity_path)


def test_export_without_encryption_key():
    """Backwards compat: no age_recipient means plain CSV output."""
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

        # Headerless: the file is exactly the data rows
        with open(output_path, 'r', newline='') as f:
            assert f.read() == 'plain@localhost,2\r\n'


def _make_db(update_time):
    """Create a mock db that returns the given UPDATE_TIME."""
    db = mock.MagicMock()
    if update_time is None:
        db.execute.return_value = iter([{'UPDATE_TIME': None}])
    else:
        db.execute.return_value = iter([{'UPDATE_TIME': update_time}])
    return db
