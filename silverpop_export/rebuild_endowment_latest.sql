-- File to rebuild the entire silverpop_endowment_latest table if required.
DROP TABLE IF EXISTS `silverpop_endowment_latest`;
CREATE TABLE IF NOT EXISTS `silverpop_endowment_latest` (
  `email` varchar(255)  PRIMARY KEY,
  `endowment_latest_currency` VARCHAR(8),
  `endowment_latest_currency_symbol` VARCHAR(8),
  `endowment_latest_native_amount` DECIMAL(20, 2),
  KEY `email` (`email`)
) COLLATE 'utf8mb4_unicode_ci';


INSERT INTO silverpop_endowment_latest
SELECT
  email.email,
  -- really we want the currency/amount associated with the highest amount on
  -- the highest date but the chances of 2 concurrent donations
  -- with different currencies are neglible
  -- so the value of handling currency better here is low.
  MAX(extra.original_currency) as endowment_latest_currency,
  MAX(cur.symbol) as endowment_latest_currency_symbol,
  MAX(extra.original_amount) as endowment_latest_native_amount
FROM silverpop_email_map t
       INNER JOIN silverpop_export_stat export ON t.email = export.email
       LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
       LEFT JOIN civicrm.civicrm_contribution c ON  c.contact_id = email.contact_id
       LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
       LEFT JOIN civicrm.civicrm_currency cur ON cur.name = extra.original_currency
WHERE c.receive_date = export.endowment_last_donation_date
  AND export.endowment_last_donation_date IS NOT NULL
  AND c.financial_type_id = 26
  AND c.contribution_status_id = 1
  AND c.total_amount > 0
GROUP BY email.email;
