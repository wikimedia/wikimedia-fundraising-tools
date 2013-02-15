UPDATE test.scratch_transactions
    SET in_civi = (
        SELECT COUNT(*) > 0 FROM wmf_contribution_extra
            WHERE gateway_txn_id = transaction_id AND gateway = 'globalcollect'
    )
    WHERE in_civi = 0;

UPDATE test.scratch_transactions
    SET in_civi = (
        SELECT IF( amount = original_amount AND currency = original_currency, 1, -1 ) FROM wmf_contribution_extra
            WHERE gateway_txn_id = transaction_id AND gateway = 'globalcollect'
    )
    WHERE in_civi = 1;

