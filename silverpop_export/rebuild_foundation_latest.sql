-- File to rebuild the entire silverpop_export_latest table if required.
DROP TABLE IF EXISTS silverpop_export_latest;
CREATE TABLE silverpop_export_latest
(
  email VARCHAR(255) PRIMARY KEY,
  latest_currency VARCHAR(3),
  latest_currency_symbol VARCHAR(8),
  latest_native_amount DECIMAL(20, 2)
) COLLATE 'utf8_unicode_ci';

INSERT INTO silverpop_export_latest
  -- temporarily specify the fields here as we no longer use latest_donation from this table
  -- and it may not be dropped on the target db yet.
(email, latest_currency, latest_currency_symbol, latest_native_amount)
SELECT
  t.email,
  MAX(extra.original_currency) as latest_currency,
  MAX(cur.symbol) as latest_currency_symbol,
  MAX(extra.original_amount) as latest_native_amount
FROM silverpop_email_map t
       INNER JOIN silverpop_export_stat export ON t.email = export.email
       LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
       LEFT JOIN civicrm.civicrm_contribution c ON  c.contact_id = email.contact_id
       LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
       LEFT JOIN civicrm.civicrm_currency cur ON cur.name = extra.original_currency
WHERE c.receive_date = export.foundation_last_donation_date
  AND c.financial_type_id <> 26
  AND c.contribution_status_id = 1
  AND c.total_amount > 0
GROUP BY t.email;
