from database.db import Connection


class Civicrm(object):
    def __init__(self, config):
        self.db = Connection(**dict(config))
        self.config = config

    def transaction_exists(self, gateway_txn_id, gateway="paypal"):
        sql = """
SELECT COUNT(*) AS count FROM wmf_contribution_extra
    WHERE gateway = %s AND gateway_txn_id = %s
        """

        count = list(self.db.execute(sql, (gateway, gateway_txn_id)))
        return count[0]['count'] > 0

    def transaction_refunded(self, gateway_txn_id, gateway="paypal"):
        sql = """
SELECT contribution_status_id AS status
    FROM wmf_contribution_extra x
    INNER JOIN civicrm_contribution c on x.entity_id = c.id
    WHERE gateway = %s AND gateway_txn_id = %s
    LIMIT 1
        """

        status = list(self.db.execute(sql, (gateway, gateway_txn_id)))
        return len(status) == 1 and status[0]['status'] == 9

    def subscription_exists(self, subscr_id):
        # FIXME: trxn_id style is inconsistent between gateways.  This will only work for paypal.
        sql = """
SELECT COUNT(*) AS count FROM civicrm_contribution_recur
    WHERE trxn_id = %s
        """

        count = list(self.db.execute(sql, (subscr_id, )))
        return count[0]['count'] > 0
