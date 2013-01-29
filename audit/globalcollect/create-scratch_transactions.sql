-- n.b. Can only be used to audit a single gateway
CREATE TABLE test.scratch_transactions (
    transaction_id VARCHAR(255) NOT NULL
        COMMENT 'FK to civicrm_contribution.trxn_id',
    received DATETIME NOT NULL,
    currency CHAR(3) NOT NULL,
    amount DECIMAL(20,2) NOT NULL,

    in_gateway TINYINT NOT NULL DEFAULT 0,
    in_civi TINYINT NOT NULL DEFAULT 0,

    PRIMARY KEY transaction_id (transaction_id),
    KEY received (received),
    KEY in_gateway (in_gateway),
    KEY in_civi (in_civi)
);
