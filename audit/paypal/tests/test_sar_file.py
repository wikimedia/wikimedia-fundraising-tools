import csv
from unittest.mock import patch
import nose.tools
import os

import audit.paypal.SarFile

# weird thing we have to do to get better assert_equals feedback
nose.tools.assert_equals.__self__.maxDiff = None


def get_csv_row(filename):
    path = os.path.dirname(__file__) + "/data/" + filename + ".csv"
    with open(path, 'r') as datafile:
        r = csv.DictReader(datafile)
        return next(r)


@patch("frqueue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals.get_config")
def test_subscr_signup(MockConfig, MockCivicrm, MockRedis):
    '''
    Test that subscr_signup messages are correctly filed
    '''
    row = get_csv_row("classic_subscr_signup")

    MockConfig.return_value.no_thankyou = False
    MockCivicrm().subscription_exists.return_value = False

    parser = audit.paypal.SarFile.SarFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    expected = {'subscr_id': 'S-7J123456DS987654B', 'txn_type': 'subscr_signup', 'currency': 'EUR', 'gross': 3.0, 'frequency_unit': 'month', 'frequency_interval': '1', 'create_date': 1493539200, 'start_date': 1493539200, 'email': 'recurring.donor@example.com', 'first_name': 'Donantus', 'last_name': 'Recurricus', 'street_address': 'Rue Faux, 41', 'city': 'Paris', 'state_province': 'Paris', 'country': 'FR', 'postal_code': '12345', 'gateway': 'paypal'}
    nose.tools.assert_equals('recurring', args[0][0])
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("frqueue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals.get_config")
def test_subscr_cancel(MockConfig, MockCivicrm, MockRedis):
    '''
    Test that subscr_cancel messages are correctly filed
    '''
    row = get_csv_row("classic_subscr_cancel")

    MockConfig.return_value.no_thankyou = False
    MockCivicrm().subscription_exists.return_value = True

    parser = audit.paypal.SarFile.SarFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    expected = {'subscr_id': 'S-7J123456DS987654B', 'txn_type': 'subscr_cancel', 'currency': 'EUR', 'gross': 3.0, 'frequency_unit': 'month', 'frequency_interval': '1', 'cancel_date': 1493539200, 'email': 'recurring.donor@example.com', 'first_name': 'Donantus', 'last_name': 'Recurricus', 'street_address': 'Rue Faux, 41', 'city': 'Paris', 'state_province': 'Paris', 'country': 'FR', 'postal_code': '12345', 'gateway': 'paypal'}
    nose.tools.assert_equals('recurring', args[0][0])
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)
