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
        "Transaction Initiation Date": "",
        "Transaction Completion Date": "",
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
    assert cm.exception.message == "Missing field subscr_id"

    # Make sure we didn't try to send anything to the queue.
    MockRedis().send.assert_has_calls([])
