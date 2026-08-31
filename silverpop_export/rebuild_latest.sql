-- File to rebuild the entire silverpop_export_latest table if required.
SELECT @recurringUpgradeType := value FROM civicrm.civicrm_option_value WHERE name = 'Recurring Upgrade';
SELECT @recurringUpgradeTypeDecline := value FROM civicrm.civicrm_option_value WHERE name = 'Recurring Upgrade Decline';
SELECT @recurringDowngradeType := value FROM civicrm.civicrm_option_value WHERE name = 'Recurring Downgrade';
SELECT @paypalProcessor := id FROM civicrm.civicrm_payment_processor WHERE name = 'paypal' AND is_test = 0;
SELECT @paypal_ecProcessor := id FROM civicrm.civicrm_payment_processor WHERE name = 'paypal_ec' AND is_test = 0;

-- Full rebuild of silverpop_has_recur for use below
DROP TABLE IF EXISTS silverpop_has_recur;
CREATE TABLE `silverpop_has_recur` (
 `email` VARCHAR(255) PRIMARY KEY,
 `foundation_has_recurred_donation` int(1) NOT NULL,
 `foundation_has_active_recurring_donation` TINYINT(1),
 `foundation_recurring_latest_donation_date` DATETIME,
 `foundation_recurring_month_latest_donation_date` DATETIME,
 `foundation_recurring_year_latest_donation_date` DATETIME,
 `foundation_recurring_first_donation_date` DATETIME,
 `foundation_recurring_active_count` TINYINT UNSIGNED,
 `foundation_recurring_latest_contribution_recur_id` INT(10),
 `recurring_has_upgrade_activity` TINYINT(1),
 `most_recent_cancel_date` DATETIME,
 `paypal_direct_recurring` TINYINT(1)
) COLLATE 'utf8mb4_unicode_ci';

DROP TEMPORARY TABLE IF EXISTS recurring_upgrade_activity_contact;
CREATE TEMPORARY TABLE recurring_upgrade_activity_contact (
  contact_id INT UNSIGNED PRIMARY KEY
) COLLATE 'utf8mb4_unicode_ci'
AS SELECT DISTINCT ac.contact_id
FROM civicrm.civicrm_activity a
INNER JOIN civicrm.civicrm_activity_contact ac ON ac.activity_id = a.id
WHERE (
    -- Either upgraded or downgraded in the last 2 years
    a.activity_type_id IN (@recurringUpgradeType, @recurringDowngradeType)
    AND a.activity_date_time > DATE_SUB(NOW(), INTERVAL 2 YEAR)
  ) OR (
    -- Or declined to upgrade in the past year
    a.activity_type_id = @recurringUpgradeTypeDecline
    AND a.activity_date_time > DATE_SUB(NOW(), INTERVAL 1 YEAR)
  );

INSERT INTO silverpop_has_recur (
  email,
  foundation_has_recurred_donation,
  foundation_has_active_recurring_donation,
  foundation_recurring_first_donation_date,
  foundation_recurring_latest_donation_date,
  foundation_recurring_month_latest_donation_date,
  foundation_recurring_year_latest_donation_date,
  -- this is used to determine the most recent cancel reason (across both fund)
  -- to avoid targeting people who have cancelled for (e.g) financial reasons
  most_recent_cancel_date,
  foundation_recurring_active_count,
  foundation_recurring_latest_contribution_recur_id,
  recurring_has_upgrade_activity,
  paypal_direct_recurring
)
 SELECT email.email,
 1 as foundation_has_recurred_donation,
 MAX(IF(
   ((end_date IS NULL OR end_date > NOW())
   AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
   AND recur.cancel_date IS NULL
   ), 1, 0)
 ) as foundation_has_active_recurring_donation,
 MIN(receive_date) as foundation_recurring_first_donation_date,
 MAX(receive_date) as foundation_recurring_latest_donation_date,
 MAX(CASE WHEN recur.frequency_unit = 'month' THEN receive_date END) as foundation_recurring_month_latest_donation_date,
 MAX(CASE WHEN recur.frequency_unit = 'year' THEN receive_date END) as foundation_recurring_year_latest_donation_date,
 MAX(recur.cancel_date) as most_recent_cancel_date,
 COUNT(DISTINCT CASE WHEN
  ((end_date IS NULL OR end_date > NOW())
   AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
   AND recur.cancel_date IS NULL
   ) THEN recur.id ELSE NULL END) as foundation_recurring_active_count,
 (-- latest active recur id if any or latest inactive recur id
 CASE WHEN COUNT(DISTINCT CASE WHEN (end_date IS NULL OR end_date > NOW())
 AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
 AND recur.cancel_date IS NULL THEN recur.id ELSE NULL END) > 0
 THEN MAX(IF(((end_date IS NULL OR end_date > NOW())
  AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
  AND recur.cancel_date IS NULL
  ), recur.id, 0)) ELSE MAX(recur.id)
  END) as foundation_recurring_latest_contribution_recur_id,
 MAX(upgrade_activity.contact_id IS NOT NULL) as recurring_has_upgrade_activity,
 MAX(CASE WHEN recur.contribution_status_id NOT IN (1, 3, 4) -- Completed,Cancelled,Failed
   AND recur.payment_processor_id IN (@paypalProcessor, @paypal_ecProcessor)
 THEN 1 ELSE 0 END) as paypal_direct_recurring
 FROM
   civicrm.civicrm_contribution_recur recur
 LEFT JOIN civicrm.civicrm_contribution contributions
   ON recur.id = contributions.contribution_recur_id
   AND contributions.contribution_status_id = 1
   AND contributions.financial_type_id != 26
   AND contributions.total_amount > 0
 INNER JOIN civicrm.civicrm_email email ON recur.contact_id = email.contact_id AND is_primary = 1
 LEFT JOIN recurring_upgrade_activity_contact upgrade_activity ON upgrade_activity.contact_id = recur.contact_id
 GROUP BY email;

DROP TEMPORARY TABLE recurring_upgrade_activity_contact;

DROP TABLE IF EXISTS silverpop_export_latest;
CREATE TABLE silverpop_export_latest
(
  email VARCHAR(255) PRIMARY KEY,
  latest_currency VARCHAR(3),
  latest_currency_symbol VARCHAR(8),
  latest_native_amount DECIMAL(20, 2),
  latest_payment_method VARCHAR(64),
  latest_donation_source VARCHAR(64),
  recurring_latest_currency VARCHAR(3),
  recurring_latest_currency_symbol VARCHAR(8),
  recurring_latest_native_amount DECIMAL(20, 2),
  recurring_latest_donation_source VARCHAR(64)
) COLLATE 'utf8mb4_unicode_ci';

-- ~40m for all contacts
INSERT INTO silverpop_export_latest
(email, latest_currency, latest_currency_symbol, latest_native_amount, latest_donation_source,
 recurring_latest_currency, recurring_latest_currency_symbol, recurring_latest_native_amount, recurring_latest_donation_source)
SELECT
  t.email,
  MAX(extra.original_currency) as latest_currency,
  MAX(cur.symbol) as latest_currency_symbol,
  MAX(extra.original_amount) as latest_native_amount,
  MAX(gift.channel) as latest_donation_source,
  MAX(recur_extra.original_currency) as recurring_latest_currency,
  MAX(recur_cur.symbol) as recurring_latest_currency_symbol,
  MAX(recur_extra.original_amount) as recurring_latest_native_amount,
  MAX(recur_gift.channel) as recurring_latest_donation_source
FROM silverpop_email_map t
   INNER JOIN silverpop_export_stat export ON t.email = export.email
   LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
   -- Most recent OTG (non-recurring) donation, matched to the precomputed OTG date.
   LEFT JOIN civicrm.civicrm_contribution c ON  c.contact_id = email.contact_id
     AND c.receive_date = export.all_funds_latest_otg_donation_date
     AND c.contribution_status_id = 1
     AND c.total_amount > 0
     AND c.contribution_recur_id IS NULL
   LEFT JOIN civicrm.civicrm_value_1_gift_data_7 gift ON gift.entity_id = c.id
   LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
   LEFT JOIN civicrm.civicrm_currency cur ON cur.name = extra.original_currency
    -- Recurring using foundation_recurring_latest_donation_date
   LEFT JOIN silverpop_has_recur hr ON hr.email = t.email
   LEFT JOIN civicrm.civicrm_contribution recur_c ON recur_c.contact_id = email.contact_id
     AND recur_c.contribution_recur_id IS NOT NULL
     AND recur_c.contribution_status_id = 1
     AND recur_c.financial_type_id != 26 -- Endowment; excluded to match foundation_recurring_latest_donation_date
     AND recur_c.total_amount > 0
     AND recur_c.receive_date = hr.foundation_recurring_latest_donation_date
   LEFT JOIN civicrm.civicrm_value_1_gift_data_7 recur_gift ON recur_gift.entity_id = recur_c.id
   LEFT JOIN civicrm.wmf_contribution_extra recur_extra ON recur_extra.entity_id = recur_c.id
   LEFT JOIN civicrm.civicrm_currency recur_cur ON recur_cur.name = recur_extra.original_currency
GROUP BY t.email;
