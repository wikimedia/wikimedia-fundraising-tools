import csv
from mock import patch
import nose.tools
import os

import audit.paypal.TrrFile

# weird thing we have to do to get better assert_equals feedback
nose.tools.assert_equals.im_class.maxDiff = None


def get_base_row():

    return {
        "Column Type": "SB",
        "Transaction ID": "AS7D98AS7D9A8S7D9AS",
        "Invoice ID": "",
        "PayPal Reference ID": "",
        "PayPal Reference ID Type": "",
        "Transaction Event Code": "",
        "Transaction Initiation Date": "2016/09/24 11:55:01 -0700",
        "Transaction Completion Date": "2016/09/24 11:55:01 -0700",
        "Transaction Debit or Credit": "",
        "Gross Transaction Amount": "1000",
        "Gross Transaction Currency": "USD",
        "Fee Debit or Credit": "",
        "Fee Amount": "55",
        "Fee Currency": "USD",
        "Transactional Status": "S",
        "Insurance Amount": "",
        "Sales Tax Amount": "0",
        "Shipping Amount": "0",
        "Transaction Subject": "",
        "Transaction Note": "",
        "Payer's Account ID": "prankster@anonymous.net",
        "Payer Address Status": "N",
        "Item Name": "Generous benificence",
        "Item ID": "DONATE",
        "Option 1 Name": "",
        "Option 1 Value": "",
        "Option 2 Name": "",
        "Option 2 Value": "",
        "Auction Site": "",
        "Auction Buyer ID": "",
        "Auction Closing Date": "",
        "Shipping Address Line1": "",
        "Shipping Address Line2": "",
        "Shipping Address City": "",
        "Shipping Address State": "",
        "Shipping Address Zip": "",
        "Shipping Address Country": "",
        "Shipping Method": "",
        "Custom Field": "1234567",
        "Billing Address Line1": "",
        "Billing Address Line2": "",
        "Billing Address City": "",
        "Billing Address State": "",
        "Billing Address Zip": "",
        "Billing Address Country": "",
        "Consumer ID": "",
        "First Name": "Banana",
        "Last Name": "Man",
        "Consumer Business Name": "",
        "Card Type": "",
        "Payment Source": "",
        "Shipping Name": "",
        "Authorization Review Status": "",
        "Protection Eligibility": "",
        "Payment Tracking ID": ""
    }


def get_refund_row():

    row = get_base_row()
    row.update({
        "PayPal Reference ID": "3GJH3GJ3334214812",
        "PayPal Reference ID Type": "TXN",
        "Transaction Event Code": "T1107",
        "Transaction Debit or Credit": "DR",
        "Fee Debit or Credit": "CR",
        "Fee Amount": "55",
        "Transaction Note": "refund",
    })
    return row


def get_recurring_row():
    row = get_base_row()
    row.update({
        "PayPal Reference ID": "3GJH3GJ3334214812",
        "PayPal Reference ID Type": "SUB",
        "Transaction Event Code": "T0002",
        "Gross Transaction Amount": "10.00",
    })
    return row


def get_csv_row(filename):
    path = os.path.dirname(__file__) + "/data/" + filename + ".csv"
    with open(path, 'r') as datafile:
        r = csv.DictReader(datafile)
        return r.next()


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
@patch("audit.paypal.paypal_api.PaypalApiClassic")
def test_recurring_charge_without_subscription(MockPaypalApi, MockGlobals, MockCivicrm, MockRedis):
    '''
    Regression test for T143903
    '''
    row = get_recurring_row()
    row["Transaction ID"] = ""
    row["PayPal Reference ID"] = ""

    MockCivicrm().transaction_exists.return_value = False

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")
    with nose.tools.assert_raises(Exception) as cm:
        parser.parse_line(row)

    # Should have failed with a specific missing field error.
    # FIXME: Annoyingly, this masks any other, unexpected exception.
    assert cm.exception.message == "Missing field subscr_id"

    # Make sure we didn't try to send anything to the queue.
    MockRedis().send.assert_has_calls([])


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_refund_send(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that we send a refund for a donation that isn't yet refunded
    '''
    row = get_refund_row()
    MockCivicrm().transaction_refunded.return_value = False

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    assert args[0][0] == 'refund'
    expected = {'last_name': 'Man', 'thankyou_date': 0, 'city': '', 'payment_method': '', 'gateway_status': 'S', 'currency': 'USD', 'postal_code': '', 'date': 1474743301, 'gateway_refund_id': 'AS7D98AS7D9A8S7D9AS', 'gateway': 'paypal', 'state_province': '', 'gross': 10.0, 'first_name': 'Banana', 'fee': 0.55, 'gateway_txn_id': 'AS7D98AS7D9A8S7D9AS', 'gross_currency': 'USD', 'country': '', 'payment_submethod': '', 'note': 'refund', 'supplemental_address_1': '', 'settled_date': 1474743301, 'gateway_parent_id': '3GJH3GJ3334214812', 'type': 'refund', 'email': 'prankster@anonymous.net', 'street_address': '', 'contribution_tracking_id': '1234567', 'order_id': '1234567'}
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_ec_donation_send(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that express checkout donations are marked as such
    '''
    row = get_csv_row("express_checkout_donation")

    MockCivicrm().transaction_exists.return_value = False

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    expected = {'last_name': 'Who', 'thankyou_date': 0, 'city': 'Whoville', 'payment_method': 'Express Checkout', 'gateway_status': 'S', 'currency': 'JPY', 'postal_code': '97211', 'date': 1488477595, 'gateway': 'paypal_ec', 'state_province': 'OR', 'gross': 150.0, 'first_name': 'Cindy Lou', 'fee': 43.0, 'gateway_txn_id': '1V551844CE5526421', 'country': 'US', 'payment_submethod': '', 'note': '', 'supplemental_address_1': '', 'settled_date': 1488477595, 'email': 'donor@generous.net', 'street_address': '321 Notta Boulevard', 'contribution_tracking_id': '46239229', 'order_id': '46239229.1'}
    nose.tools.assert_equals('donations', args[0][0])
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_ec_recurring_donation_send(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that express checkout recurring donations are marked as such
    '''
    row = get_csv_row("express_checkout_recurring_donation")

    MockCivicrm().transaction_exists.return_value = False

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    expected = {'txn_type': 'subscr_payment', 'subscr_id': 'I-SS5RD7POSD46', 'last_name': 'Who', 'thankyou_date': 0, 'city': '', 'payment_method': 'Others', 'gateway_status': 'S', 'currency': 'JPY', 'postal_code': '', 'date': 1488634565, 'gateway': 'paypal_ec', 'state_province': '', 'gross': 150.0, 'first_name': 'Cindy Lou', 'fee': 43.0, 'gateway_txn_id': '4JH2438EE9876546W', 'country': '', 'payment_submethod': '', 'note': '', 'supplemental_address_1': '', 'settled_date': 1488634565, 'email': 'donor@generous.net', 'street_address': '', 'contribution_tracking_id': '45931681', 'order_id': '45931681.1'}
    nose.tools.assert_equals('recurring', args[0][0])
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_ec_refund_send(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that express checkout refunds are marked as such
    '''
    row = get_csv_row("express_checkout_refund")

    MockCivicrm().transaction_refunded.return_value = False

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    expected = {'last_name': 'Who', 'thankyou_date': 0, 'city': 'Whoville', 'payment_method': 'Others', 'gateway_status': 'S', 'currency': 'JPY', 'postal_code': '97211', 'date': 1490200499, 'gateway_refund_id': '3HD08833MR473623T', 'gateway': 'paypal_ec', 'state_province': 'OR', 'gross': 150.0, 'first_name': 'Cindy Lou', 'fee': 43.0, 'gateway_txn_id': '3HD08833MR473623T', 'gross_currency': 'JPY', 'country': 'US', 'payment_submethod': '', 'note': 'refund', 'supplemental_address_1': '', 'settled_date': 1490200499, 'gateway_parent_id': '1V551844CE5526421', 'type': 'refund', 'email': 'donor@generous.net', 'street_address': '321 Notta Boulevard', 'contribution_tracking_id': '46239229', 'order_id': '46239229.1'}
    assert args[0][0] == 'refund'
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_ec_recurring_refund_send(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that express checkout recurring refunds are marked as ec too
    '''
    row = get_csv_row("express_checkout_recurring_refund")

    MockCivicrm().transaction_refunded.return_value = False

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    expected = {'last_name': 'Who', 'thankyou_date': 0, 'city': '', 'payment_method': 'Others', 'gateway_status': 'S', 'currency': 'JPY', 'postal_code': '', 'date': 1490200431, 'gateway_refund_id': '8WG23468CX793000L', 'gateway': 'paypal_ec', 'state_province': '', 'gross': 150.0, 'first_name': 'Cindy Lou', 'fee': 43.0, 'gateway_txn_id': '8WG23468CX793000L', 'gross_currency': 'JPY', 'country': '', 'payment_submethod': '', 'note': 'refund', 'supplemental_address_1': '', 'settled_date': 1490200431, 'gateway_parent_id': '4JH2438EE9876546W', 'type': 'refund', 'email': 'donor@generous.net', 'street_address': '', 'contribution_tracking_id': '45931681', 'order_id': '45931681.1'}
    assert args[0][0] == 'refund'
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_refund_duplicate(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that we do not send a refund for a donation that is refunded
    '''
    row = get_refund_row()
    MockCivicrm().transaction_refunded.return_value = True

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    MockRedis().send.assert_has_calls([])


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_recurring(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that we send a normalized message for recurring donations
    '''
    MockCivicrm().transaction_exists.return_value = False

    row = get_recurring_row()
    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    assert args[0][0] == 'recurring'
    expected = {'last_name': 'Man', 'txn_type': 'subscr_payment', 'thankyou_date': 0, 'city': '', 'payment_method': '', 'gateway_status': 'S', 'currency': 'USD', 'postal_code': '', 'date': 1474743301, 'subscr_id': '3GJH3GJ3334214812', 'gateway': 'paypal', 'state_province': '', 'gross': 0.1, 'first_name': 'Banana', 'fee': 0.55, 'gateway_txn_id': 'AS7D98AS7D9A8S7D9AS', 'country': '', 'payment_submethod': '', 'note': '', 'supplemental_address_1': '', 'settled_date': 1474743301, 'email': 'prankster@anonymous.net', 'street_address': '', 'contribution_tracking_id': '1234567', 'order_id': '1234567'}
    actual = args[0][1]
    nose.tools.assert_equals(expected, actual)


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_duplicate_recurring(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that we don't send duplicate recurring messages
    '''
    MockCivicrm().transaction_exists.return_value = True

    row = get_recurring_row()
    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    args = MockRedis().send.call_args
    nose.tools.assert_equals(None, args)
