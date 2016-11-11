from mock import patch
import nose.tools

import audit.paypal.TrrFile


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
@patch("audit.paypal.paypal_api.PaypalApiClassic")
def test_recurring_charge_without_subscription(MockPaypalApi, MockGlobals, MockCivicrm, MockRedis):
    '''
    Regression test for T143903
    '''
    row = {
        "Column Type": "",
        "Transaction ID": "",
        "Invoice ID": "",
        "PayPal Reference ID": "",
        "PayPal Reference ID Type": "SUB",
        "Transaction Event Code": "T0002",
        # FIXME: Use and test real date format.
        "Transaction Initiation Date": "Thu Oct 13 19:53:57 UTC 2016",
        "Transaction Completion Date": "Thu Oct 13 19:53:58 UTC 2016",
        # sic.
        "Transaction  Debit or Credit": "",
        "Gross Transaction Amount": "10.00",
        "Gross Transaction Currency": "",
        "Fee Debit or Credit": "",
        "Fee Amount": "",
        "Fee Currency": "",
        "Transactional Status": "",
        "Insurance Amount": "",
        "Sales Tax Amount": "",
        "Shipping Amount": "",
        "Transaction Subject": "",
        "Transaction Note": "",
        "Payer's Account ID": "",
        "Payer Address Status": "",
        "Item Name": "",
        "Item ID": "",
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
        "Custom Field": "",
        "Billing Address Line1": "",
        "Billing Address Line2": "",
        "Billing Address City": "",
        "Billing Address State": "",
        "Billing Address Zip": "",
        "Billing Address Country": "",
        "Consumer ID": "",
        "First Name": "Malcolm",
        "Last Name": "3X",
        "Consumer Business Name": "",
        "Card Type": "",
        "Payment Source": "",
        "Shipping Name": "",
        "Authorization Review Status": "",
        "Protection Eligibility": "",
        "Payment Tracking ID": "",
    }
    MockCivicrm().transaction_exists.return_value = False
    MockPaypalApi().fetch_donor_name.return_value = ("Malcolm", "3X")

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")
    with nose.tools.assert_raises(Exception) as cm:
        parser.parse_line(row)

    # Should have failed with a specific missing field error.
    # FIXME: Annoyingly, this masks any other, unexpected exception.
    assert cm.exception.message == "Missing field subscr_id"

    # Make sure we didn't try to send anything to the queue.
    MockRedis().send.assert_has_calls([])


def get_refund_row():

    return {
        "Column Type": "SB",
        "Transaction ID": "AS7D98AS7D9A8S7D9AS",
        "Invoice ID": "",
        "PayPal Reference ID": "3GJH3GJ3334214812",
        "PayPal Reference ID Type": "TXN",
        "Transaction Event Code": "T1107",
        "Transaction Initiation Date": "2016/09/24 11:55:01 -0700",
        "Transaction Completion Date": "2016/09/24 11:55:01 -0700",
        "Transaction  Debit or Credit": "DR",
        "Gross Transaction Amount": "1000",
        "Gross Transaction Currency": "USD",
        "Fee Debit or Credit": "CR",
        "Fee Amount": "55",
        "Fee Currency": "USD",
        "Transactional Status": "S",
        "Insurance Amount": "",
        "Sales Tax Amount": "0",
        "Shipping Amount": "0",
        "Transaction Subject": "",
        "Transaction Note": "r",
        "Payer's Account ID": "prankster@anonymous.net",
        "Payer Address Status": "N",
        "Item Name": "Generous benificence",
        "Item ID": "GIMME",
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
        "Payment Tracking ID": "",
    }


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
    expected = sorted({'last_name': 'Man', 'thankyou_date': 0, 'city': '', 'payment_method': '', 'gateway_status': 'S', 'currency': 'USD', 'postal_code': '', 'date': '1474736101', 'gateway_refund_id': 'AS7D98AS7D9A8S7D9AS', 'gateway': 'paypal', 'state_province': '', 'gross': 10.0, 'first_name': 'Banana', 'fee': 0.55, 'gateway_txn_id': 'AS7D98AS7D9A8S7D9AS', 'gross_currency': 'USD', 'country': '', 'payment_submethod': '', 'note': 'r', 'supplemental_address_1': '', 'settled_date': '1474736101', 'gateway_parent_id': '3GJH3GJ3334214812', 'type': 'refund', 'email': 'prankster@anonymous.net', 'street_address': ''})
    actual = sorted(args[0][1])
    assert actual == expected


@patch("queue.redis_wrap.Redis")
@patch("civicrm.civicrm.Civicrm")
@patch("process.globals")
def test_refund_duplicate(MockGlobals, MockCivicrm, MockRedis):
    '''
    Test that we send a refund for a donation that isn't yet refunded
    '''
    row = get_refund_row()
    MockCivicrm().transaction_refunded.return_value = True

    parser = audit.paypal.TrrFile.TrrFile("dummy_path")

    parser.parse_line(row)

    # Did we send it?
    MockRedis().send.assert_has_calls([])
