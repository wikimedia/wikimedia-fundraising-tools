SET autocommit = 1;
SELECT @recurringUpgradeType := value FROM civicrm.civicrm_option_value WHERE name = 'Recurring Upgrade';
SELECT @recurringUpgradeTypeDecline := value FROM civicrm.civicrm_option_value WHERE name = 'Recurring Upgrade Decline';
-- Updates the silverpop_export table

-- Explanation of tables (as of now, still being re-worked).
-- silverpop_export_staging - summarised contact data with complexities around country, language, opt in, opt out resolved
-- silverpop_missing_countries - support table for building the above table
-- silverpop_email_map - summary table of contact data where we want 'the one that has this data', provides master_id
--    for later filtering.
-- silverpop_export_stat aggregate data about contact's contibutions
-- silverpop_export_latest - data about contact's most recent foundation donation
-- silverpop_export_highest - data about contact's highest foundation donation
-- silverpop_endowment_latest - data about contact's most recent endowment donation
-- silverpop_endowment_highest - data about contact's highest endowment donation
-- silverpop_export - collation of data from above tables
-- silverpop_export_view - collation of data from above tables + formatting.
-- silverpop_update_world - table of emails updated in our update timeframe. Only emails from this table
--    need to be changed in our incremental update.
-- silverpop_countrylangs - look up of our best guess of the language associated with the donor's country if we
--   don't know their language

-- The point of silverpop_export is presumably that it is more performant than skipping straight to silverpop_export_view
-- although I believe that theory needs testing.

-- Rebuild stats table routine
-- this should go in it's own file but will create complexities around other
-- unmerged commits so not at this stage.
-- Note the whole thing is in a transaction so it always has integrity.
BEGIN;
  -- Delete stats for any rows in our change set (means we just need to insert
  -- Query OK, 776384 rows affected (47.62 sec)
  DELETE stat FROM silverpop_update_world t INNER JOIN silverpop_export_stat stat ON t.email = stat.email;

  -- INSERT new contact rows into export stats table
  -- following timing on staging with 7 days - likely similar to peak volume with a shorter period.
  -- Query OK, 776383 rows affected (1 min 25.41 sec)
  INSERT INTO silverpop_export_stat (
   email,
   all_funds_latest_donation_date,
   foundation_lifetime_usd_total,
   foundation_donation_count,
   foundation_first_donation_date,
   foundation_last_donation_date,
   foundation_highest_usd_amount,
   endowment_highest_usd_amount,
   endowment_last_donation_date,
   endowment_first_donation_date,
   endowment_number_donations,
   donor_segment_id,
   donor_status_bin,
   all_funds_total_2018_2019,
   all_funds_total_2019_2020,
   all_funds_total_2020_2021,
   all_funds_total_2021_2022,
   all_funds_total_2022_2023,
   all_funds_total_2023_2024,
   all_funds_total_2024_2025,
   all_funds_total_2025_2026
  )
  SELECT
    e.email,
    MAX(IF (donor.endowment_last_donation_date IS NULL OR last_donation_date > donor.endowment_last_donation_date , last_donation_date, donor.endowment_last_donation_date)) as all_funds_latest_donation_date,
    COALESCE(SUM(donor.lifetime_usd_total), 0) as foundation_lifetime_usd_total,
    COALESCE(SUM(donor.number_donations), 0) as foundation_donation_count,
    MIN(donor.first_donation_date) as foundation_first_donation_date,
    MAX(donor.last_donation_date) as foundation_last_donation_date,
    MAX(donor.largest_donation) as foundation_highest_usd_amount,
    MAX(donor.endowment_largest_donation) as endowment_highest_usd_amount,
    MAX(donor.endowment_last_donation_date) as endowment_last_donation_date,
    MIN(donor.endowment_first_donation_date) as endowment_first_donation_date,
    COALESCE(SUM(donor.endowment_number_donations), 0) as endowment_number_donations,
    -- we use MIN because the higher priority values are lower - ie Major Donor is 100 and
    -- mid-tier is 200. If combining 2 merge-like donors we want to treat them as Major Donor
    -- if choosing between 100 & 200. We need to be careful with field types here. At this stage
    -- the code is pushing up the value (ie the 100) but it is likely we will be asked to
    -- push up the field name (Major Donor) - hence the type is varchar. But, we need to do
    -- any comparisons directly on the wmf_donor table, where it is an int.
    MIN(donor.donor_segment_id) as donor_segment,
    -- Status values are trickier - if we want to combine one lybunt (35) record
    -- and one new (25) record, the correct answer is 'consecutive' (20). So
    -- we translate to bitwise flags for the merge, then check flags in the
    -- output view.
    BIT_OR(
      CASE
        WHEN donor.donor_status_id =  2 THEN 256 -- B'100000000'
        WHEN donor.donor_status_id =  4 THEN 128 -- B'010000000'
        WHEN donor.donor_status_id =  6 THEN  64 -- B'001000000'
        WHEN donor.donor_status_id =  8 THEN  32 -- B'000100000'
        WHEN donor.donor_status_id = 20 THEN  24 -- B'000011000'
        WHEN donor.donor_status_id = 25 THEN  16 -- B'000010000'
        WHEN donor.donor_status_id = 30 THEN  20 -- B'000010100'
        WHEN donor.donor_status_id = 35 THEN   8 -- B'000001000'
        WHEN donor.donor_status_id = 50 THEN   4 -- B'000000100'
        WHEN donor.donor_status_id = 60 THEN   2 -- B'000000010'
        WHEN donor.donor_status_id = 70 THEN   1 -- B'000000001'
        WHEN donor.donor_status_id = 1000 THEN 0
        ELSE 0
      END
    ) as donor_status_bin,
    COALESCE(SUM(donor.all_funds_total_2018_2019), 0) as all_funds_total_2018_2019,
    COALESCE(SUM(donor.all_funds_total_2019_2020), 0) as all_funds_total_2019_2020,
    COALESCE(SUM(donor.all_funds_total_2020_2021), 0) as all_funds_total_2020_2021,
    COALESCE(SUM(donor.all_funds_total_2021_2022), 0) as all_funds_total_2021_2022,
    COALESCE(SUM(donor.all_funds_total_2022_2023), 0) as all_funds_total_2022_2023,
    COALESCE(SUM(donor.all_funds_total_2023_2024), 0) as all_funds_total_2023_2024,
    COALESCE(SUM(donor.all_funds_total_2024_2025), 0) as all_funds_total_2024_2025,
    COALESCE(SUM(donor.all_funds_total_2025_2026), 0) as all_funds_total_2025_2026
  FROM silverpop_update_world t
    INNER JOIN civicrm.civicrm_email e FORCE INDEX(UI_email) ON e.email = t.email
      AND e.is_primary = 1
    LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = e.contact_id
    # We need to be careful with this group by. We want the sum by email but we do not want
    # any other left joins that could be 1 to many & inflate the aggregates.
  GROUP BY e.email;

COMMIT;


-- Query OK, 23199001 rows affected (11 min 55.19 sec)
INSERT INTO silverpop_email_map (
  email,
  master_email_id,
  address_id,
  preferred_language,
  opted_out,
  opted_in,
  modified_date
)
  SELECT ex.email,
    COALESCE(MAX(if(ex.all_funds_latest_donation_date = stat.all_funds_latest_donation_date, ex.id, NULL)), MAX(id)) as master_email_id,
    COALESCE(MAX(if(ex.all_funds_latest_donation_date = stat.all_funds_latest_donation_date, ex.address_id, NULL)), MAX(address_id)) as address_id,
    # Use MAX to prefer non-blank
    MAX(preferred_language) as preferred_language,
    # Use MAX as any opted out IS opted out.
    MAX(opted_out) as opted_out,
    # 0 if they have ever actually opted out, else 1
    # we use this for filtering so do not need to preserve the nuance.
    # This should be revisited per https://phabricator.wikimedia.org/T256522
    MIN(IF (opted_in = 0, 0, 1)) as opted_in,
    MAX(modified_date) as modified_date
  FROM silverpop_export_staging ex
  INNER JOIN silverpop_export_stat stat
    ON ex.email = stat.email
  GROUP BY ex.email
;

-- Find the latest donation for each email address. Ordering by
-- receive_date and total_amount descending should always insert
-- the latest donation first, with the larger prevailing for an
-- email with multiple simultaneous donations. All the rest for
-- that email will be ignored due to the unique constraint. We
-- use 'ON DUPLICATE KEY UPDATE' instead of 'INSERT IGNORE' as
-- the latter throws warnings.
BEGIN;
-- Delete recent rows from latest table (make way for updated version).
-- Query OK, 679292 rows affected (4.12 sec)
DELETE latest FROM silverpop_update_world t INNER JOIN silverpop_export_latest latest ON t.email = latest.email;
-- Add recent rows to latest export table
-- Query OK, 679292 rows affected (24.34 sec)
INSERT INTO silverpop_export_latest (
   email,
   latest_currency,
   latest_currency_symbol,
   latest_native_amount,
   latest_donation_source
)
  SELECT
    t.email,
    MAX(extra.original_currency) as latest_currency,
    MAX(cur.symbol) as latest_currency_symbol,
    MAX(extra.original_amount) as latest_native_amount,
    MAX(gift.channel)  as latest_donation_source
  FROM silverpop_update_world t
    INNER JOIN silverpop_export_stat export ON t.email = export.email
    LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
    LEFT JOIN civicrm.civicrm_contribution c ON c.contact_id = email.contact_id
    LEFT JOIN civicrm.civicrm_value_1_gift_data_7 gift ON gift.entity_id = c.id
    LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
    LEFT JOIN civicrm.civicrm_currency cur ON cur.name = extra.original_currency
    WHERE c.receive_date = export.foundation_last_donation_date
    AND c.financial_type_id <> 26
    AND c.contribution_status_id = 1
    AND c.total_amount > 0
    GROUP BY t.email;
COMMIT;


-- Populate table for highest donation amount and date
BEGIN;
-- Delete recent rows from highest table (make way for updated version).
-- Query OK, 679293 rows affected (4.27 sec)
DELETE highest FROM silverpop_update_world t INNER JOIN silverpop_export_highest highest ON t.email = highest.email;
-- Add recent rows to highest export table
-- Query OK, 679293 rows affected, 12 warnings (1 min 15.22 sec)
INSERT INTO silverpop_export_highest (
  email,
  highest_native_currency,
  highest_native_amount,
  highest_usd_amount,
  highest_donation_date
)
  SELECT
    e.email,
    ex.original_currency,
    ex.original_amount,
    ct.total_amount,
    ct.receive_date
   FROM silverpop_update_world t
     INNER JOIN silverpop_export_staging e ON t.email = e.email,
    civicrm.civicrm_contribution ct,
    civicrm.wmf_contribution_extra ex
  WHERE
    e.contact_id = ct.contact_id AND
    ex.entity_id = ct.id AND
    ct.receive_date IS NOT NULL AND
    ct.total_amount > 0 AND -- Refunds don't count
    ct.contribution_status_id = 1 AND-- 'Completed'
    ct.financial_type_id <> 26 -- endowments
  ORDER BY
    ct.total_amount DESC,
    ct.receive_date DESC
ON DUPLICATE KEY UPDATE highest_native_currency = silverpop_export_highest.highest_native_currency;
COMMIT;

BEGIN;
-- Delete recent rows from endowment_latest table (make way for updated version).
-- Query OK, 73566 rows affected (0.72 sec)
DELETE latest FROM silverpop_update_world t INNER JOIN silverpop_endowment_latest latest ON t.email = latest.email;
-- Add recent rows to endowment_latest table
-- Query OK, 73566 rows affected (50.44 sec)
INSERT INTO silverpop_endowment_latest (
  email,
  endowment_latest_currency,
  endowment_latest_currency_symbol,
  endowment_latest_native_amount,
  endowment_latest_donation_source
)
SELECT
  email.email,
  -- really we want the currency/amount associated with the highest amount on
  -- the highest date but the chances of 2 concurrent donations
  -- with different currencies are negligible
  -- so the value of handling currency better here is low.
  MAX(extra.original_currency) as endowment_latest_currency,
  MAX(cur.symbol) as endowment_latest_currency_symbol,
  MAX(extra.original_amount) as endowment_latest_native_amount,
  MAX(gift.channel) as endowment_latest_donation_source
FROM silverpop_update_world t
        INNER JOIN silverpop_export_stat export ON t.email = export.email
        LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
        LEFT JOIN civicrm.civicrm_contribution c ON  c.contact_id = email.contact_id
        LEFT JOIN civicrm.civicrm_value_1_gift_data_7 gift ON gift.entity_id = c.id
        LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
        LEFT JOIN civicrm.civicrm_currency cur ON cur.name = extra.original_currency
WHERE c.receive_date = export.endowment_last_donation_date
  AND export.endowment_last_donation_date IS NOT NULL
  AND c.financial_type_id = 26
  AND c.contribution_status_id = 1
  AND c.total_amount > 0
GROUP BY email.email;
COMMIT;

BEGIN;
-- Delete recent rows from endowment_highest table (make way for updated version).
-- Query OK, 73565 rows affected (0.51 sec)
DELETE highest FROM silverpop_update_world t INNER JOIN silverpop_endowment_highest highest ON t.email = highest.email;
-- Add recent rows to endowment_highest table
-- Query OK, 73565 rows affected (47.86 sec)
INSERT INTO silverpop_endowment_highest (
  email,
  endowment_highest_donation_date,
  endowment_highest_native_currency,
  endowment_highest_native_amount
)
SELECT
  email.email,
  MAX(c.receive_date) as endowment_highest_donation_date,
  -- really we want the currency/amount associated with the highest amount on
  -- the highest date but the chances of 2 concurrent donations
  -- with different currencies are negligible
  -- so the value of handling currency better here is low.
  MAX(extra.original_currency) as endowment_highest_native_currency,
  MAX(extra.original_amount) as endowment_highest_native_amount
FROM silverpop_update_world t
  INNER JOIN silverpop_export_stat export ON t.email = export.email
  LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
  LEFT JOIN civicrm.civicrm_contribution c ON  c.contact_id = email.contact_id
  LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
WHERE c.total_amount = export.endowment_highest_usd_amount
  AND export.endowment_highest_usd_amount > 0
  AND c.financial_type_id = 26
  AND c.contribution_status_id = 1
GROUP BY email.email;
COMMIT;

BEGIN;
-- Delete recent rows from has_recur table (make way for updated version).
-- Query OK, 94904 rows affected (0.61 sec)
DELETE recur FROM silverpop_update_world t INNER JOIN silverpop_has_recur recur ON t.email = recur.email;
-- Add recent rows to has_recur table
-- Query OK, 134000 rows affected (38.378 sec)
INSERT INTO silverpop_has_recur (
  email,
  foundation_has_recurred_donation,
  foundation_has_active_recurring_donation,
  foundation_recurring_first_donation_date,
  foundation_recurring_latest_donation_date,
  -- this is used to determine the most recent cancel reason (across both fund)
  -- to avoid targeting people who have cancelled for (e.g) financial reasons
  most_recent_cancel_date,
  foundation_recurring_active_count,
  foundation_recurring_latest_contribution_recur_id,
  recurring_has_upgrade_activity
)
 SELECT DISTINCT email.email,
 1 as foundation_has_recurred_donation,
 MAX(IF(
   ((end_date IS NULL OR end_date > NOW())
   AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
   AND recur.cancel_date IS NULL
   ), 1, 0)
 ) as foundation_has_active_recurring_donation,
 MIN(receive_date) as `foundation_recurring_first_donation_date`,
 MAX(receive_date) as `foundation_recurring_latest_donation_date`,
 MAX(recur.cancel_date) as most_recent_cancel_date,
 COUNT(DISTINCT CASE WHEN
  ((end_date IS NULL OR end_date > NOW())
   AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
   AND recur.cancel_date IS NULL
   ) THEN recur.id ELSE NULL END) as foundation_recurring_active_count,
 (-- latest active recur id if any or latest inactive recur id
 CASE WHEN COUNT(DISTINCT CASE WHEN (end_date IS NULL OR end_date > NOW())
 AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
 AND recur.cancel_date IS NULL > 0 THEN recur.id ELSE NULL END) > 0
 THEN MAX(IF(((end_date IS NULL OR end_date > NOW())
  AND recur.contribution_status_id NOT IN(1,3,4) -- Completed,Cancelled,Failed
  AND recur.cancel_date IS NULL
  ), recur.id, 0)) ELSE MAX(recur.id)
  END) as foundation_recurring_latest_contribution_recur_id,
 ( -- Hat tip to Eileen
   SELECT count(*) > 0
   FROM civicrm.civicrm_activity_contact ac
     INNER JOIN civicrm.civicrm_activity a
         ON a.id = ac.activity_id
            AND (
                -- Either upgraded at any time in the past
                a.activity_type_id = @recurringUpgradeType OR (
                    -- Or declined to upgrade in the past year
                    a.activity_type_id = @recurringUpgradeTypeDecline AND
                    a.activity_date_time > DATE_SUB(NOW(), INTERVAL 1 YEAR)
                )
            )
   WHERE ac.contact_id = recur.contact_id
 ) as recurring_has_upgrade_activity
 FROM
   civicrm.civicrm_contribution_recur recur
 LEFT JOIN civicrm.civicrm_contribution contributions
   ON recur.id = contributions.contribution_recur_id
   AND contributions.contribution_status_id = 1
   AND contributions.financial_type_id != 26
   AND contributions.total_amount > 0
 INNER JOIN civicrm.civicrm_email email ON recur.contact_id = email.contact_id AND is_primary = 1
 INNER JOIN silverpop_update_world t ON t.email = email.email
 GROUP BY email;
COMMIT;

DELETE cancel FROM silverpop_update_world t INNER JOIN cancel_reason cancel ON t.email = cancel.email;
INSERT INTO cancel_reason
SELECT recur.email,
  -- this list is defined / hard-coded in WMFHook/QuickForm
  -- the use of MAX here is on the off change there are 2 cancel reasons
  -- on exactly the same date & for FULL GROUP BY compliance - but it does nothing really.
  MAX(
  CASE
    WHEN r.cancel_reason = 'Other And Unspecified' THEN 'other_and_unspecified'
    WHEN r.cancel_reason = 'Financial Reasons' THEN 'financial_reasons'
    WHEN r.cancel_reason = 'Duplicate recurring donation' THEN 'duplicate_recurring_donation'
    WHEN r.cancel_reason = 'Wikipedia content related complaint' THEN 'wikipedia_content_related_complaint'
    WHEN r.cancel_reason = 'Wikimedia Foundation related complaint' THEN 'wikimedia_foundation_related_complaint'
    WHEN r.cancel_reason = 'Lack of donation management tools' THEN 'lack_of_donation_management_tools'
    WHEN r.cancel_reason = 'Matching Gift' THEN 'matching_gift'
    WHEN r.cancel_reason = 'Unintended recurring donation' THEN 'unintended_recurring_donation'
    WHEN r.cancel_reason = 'Chapter' THEN 'chapter'
    WHEN r.cancel_reason = 'Update' THEN 'update'
    WHEN r.cancel_reason = 'Frequency' THEN 'frequency'
    ELSE 'not_communicated_lapsed'
  END) as most_recent_cancel_reason
FROM silverpop_has_recur recur
INNER JOIN civicrm.civicrm_email e
  ON e.email = recur.email AND is_primary = 1
INNER JOIN silverpop_update_world t ON t.email = e.email
INNER JOIN civicrm.civicrm_contribution_recur r
  ON r.contact_id = e.contact_id AND cancel_date IS NOT NULL
WHERE most_recent_cancel_date > DATE_SUB(foundation_recurring_latest_donation_date, INTERVAL 6 WEEK)
  AND most_recent_cancel_date = r.cancel_date
-- DR were not entering this in a curated way before June / July 2024
-- so any data before that is likely inaccurate - it is probably easier
-- to leave out for now & add more in if stakeholders confirm they want more.
  AND r.cancel_date > '2024-06-01'
GROUP BY recur.email;

BEGIN;
-- Delete recent rows from export table (make way for updated version).
-- Query OK, 653187 rows affected (10.02 sec)
DELETE export FROM silverpop_update_world t INNER JOIN silverpop_export export ON t.email = export.email;

-- Delete any rows that have been removed from the silverpop_export_staging table.
-- rows would only ever have been added to this table based on them being in
-- the staging export table so clearing them out, when gone, makes sense.
DELETE export FROM silverpop_export export
  LEFT JOIN silverpop_export_staging s ON s.id = export.id WHERE s.id IS NULL;

-- Delete rows where based on the id having a recently modified date.
-- If the email changed from one email to another the email based delete will not pick it up.
-- Query OK, 161272 rows affected (5.93 sec)
DELETE export FROM silverpop_export_staging t INNER JOIN silverpop_export export ON t.id = export.id
WHERE t.modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY);

-- Delete rows based on contact_id having a recently modified_date
-- This addresses the situation where the primary email of the contact has changed
-- and there may be a row associated with the old contact_id.
-- Query OK, 2017 rows affected (1.32 sec)
DELETE export FROM silverpop_export_staging t INNER JOIN silverpop_export export ON t.contact_id = export.contact_id
WHERE t.modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY);

-- Move the data from the staging table into the persistent one
-- Query OK, 653187 rows affected (50.32 sec)
INSERT INTO silverpop_export (
  id,modified_date, contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in, employer_id, employer_name,
  -- has recurred isn't really used now - I'm just a bit reluctant to remove it in case they want it back.
  foundation_has_recurred_donation,
  foundation_has_active_recurring_donation,
  foundation_recurring_first_donation_date,
  foundation_recurring_latest_donation_date,
  foundation_recurring_active_count,
  recurring_has_upgrade_activity,
  foundation_recurring_latest_contribution_recur_id,
  foundation_highest_usd_amount,foundation_highest_native_amount,
  foundation_highest_native_currency,foundation_highest_donation_date,lifetime_usd_total,donation_count,
  foundation_latest_currency,foundation_latest_currency_symbol,foundation_latest_native_amount,
  foundation_last_donation_date, foundation_first_donation_date,
  city,country,state,postal_code,
  donor_segment_id, donor_status_bin,
  endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations, endowment_highest_usd_amount,
  all_funds_total_2018_2019,
  all_funds_total_2019_2020,
  all_funds_total_2020_2021,
  all_funds_total_2021_2022,
  all_funds_total_2022_2023,
  all_funds_total_2023_2024,
  all_funds_total_2024_2025,
  all_funds_total_2025_2026
)
SELECT ex.id, dedupe_table.modified_date, ex.contact_id,ex.contact_hash,ex.first_name,ex.last_name,
  -- get the one associated with the master email, failing that 'any'
  COALESCE(ex.preferred_language, dedupe_table.preferred_language) as preferred_language,
  ex.email,ex.opted_in, ex.employer_id, ex.employer_name,
  foundation_has_recurred_donation,
  foundation_has_active_recurring_donation,
  foundation_recurring_first_donation_date,
  foundation_recurring_latest_donation_date,
  foundation_recurring_active_count,
  recurring_has_upgrade_activity,
  foundation_recurring_latest_contribution_recur_id,
  COALESCE(hg.highest_usd_amount, 0) as foundation_highest_usd_amount,
  COALESCE(hg.highest_native_amount, 0) as foundation_highest_native_amount,
  COALESCE(hg.highest_native_currency, '') as foundation_highest_native_currency,
  hg.highest_donation_date as foundation_highest_donation_date,
  COALESCE(foundation_lifetime_usd_total, 0) as foundation_lifetime_usd_total,
  COALESCE(foundation_donation_count, 0) as foundation_donation_count,
  lt.latest_currency as foundation_latest_currency,
  lt.latest_currency_symbol as foundation_latest_currency_symbol,
  COALESCE(lt.latest_native_amount, 0) as foundation_latest_native_amount,
  foundation_last_donation_date,foundation_first_donation_date,
  addr.city,addr.country,addr.state,addr.postal_code,
  stats.donor_segment_id, stats.donor_status_bin,
  endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations,
  COALESCE(endowment_highest_usd_amount,0) as endowment_highest_usd_amount,
   stats.all_funds_total_2018_2019,
   stats.all_funds_total_2019_2020,
   stats.all_funds_total_2020_2021,
   stats.all_funds_total_2021_2022,
   stats.all_funds_total_2022_2023,
   stats.all_funds_total_2023_2024,
   stats.all_funds_total_2024_2025,
   stats.all_funds_total_2025_2026
FROM silverpop_update_world t
INNER JOIN silverpop_export_staging ex ON t.email = ex.email

-- this inner join is restricting us to only one record per email.
-- currently it is the highest email_id. Ideally it will later to change to
-- email_id associated with the highest donation.
INNER JOIN silverpop_email_map dedupe_table ON ex.id = dedupe_table.master_email_id
INNER JOIN silverpop_export_stat stats ON stats.email = dedupe_table.email
LEFT JOIN silverpop_has_recur recur ON recur.email = dedupe_table.email
LEFT JOIN silverpop_export_latest lt ON ex.email = lt.email
LEFT JOIN silverpop_export_highest hg ON ex.email = hg.email
LEFT JOIN silverpop_export_staging addr ON dedupe_table.address_id = addr.address_id

-- using dedupe_table gets the 'max' - ie if ANY are 1 then we get that.
WHERE dedupe_table.opted_out=0
AND (ex.opted_in IS NULL OR ex.opted_in = 1)
ON DUPLICATE KEY UPDATE silverpop_export.id=ex.id;

-- Delete rows then recreate so we don't include people we're deleting from the main table.
DELETE FROM silverpop_export_checksum_email;

-- Move the data from the staging table into the persistent one
-- Query OK, 28373047 rows in set (31.883 sec)
INSERT INTO silverpop_export_checksum_email (
  email,
  checksum
)
SELECT
    email,
    CONCAT(MD5(CONCAT(contact_hash, '_', contact_id, '_', UNIX_TIMESTAMP(), '_', '1440')),"_",UNIX_TIMESTAMP(),"_","1440")
FROM
    silverpop_export
COMMIT;

-- create preference_tags table. Currently, this is basically instant
-- with only a few hundred tagged contacts so drop & create
-- for simplicity.
-- Query OK, 325 rows affected (0.014 sec)
DROP TABLE IF EXISTS preference_tags;

CREATE TABLE preference_tags
(email VARCHAR(64) NOT NULL, INDEX(email), preference_tags VARCHAR(258))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
SELECT email, GROUP_CONCAT(DISTINCT TRIM(REPLACE(tag.label, 'Preference: ', '')) SEPARATOR ';') as preference_tags
FROM civicrm.civicrm_entity_tag e
  INNER JOIN civicrm.civicrm_tag tag
    ON e.tag_id = tag.id
    AND tag.label LIKE 'Preference: %'
    AND tag.used_for = 'civicrm_contact'
  INNER JOIN civicrm.civicrm_email email
    ON email.contact_id = e.entity_id
    AND email.is_primary = 1
GROUP BY email.email;

-- Create a nice view to export from
-- There are two possibilities for limiting this view to only include newly modified contacts
-- add a where statement or join on an already-limited table.
--
-- For the former I worry there could be timing integrity issues - this is true to the silverpop_export
-- table at the time it was last updated. But if the next silverpop started the
-- data in the silverpop_update_world table could be out of sync - ie if it had been
-- recreated for the following day.
--
-- So I want to add a where statement to the view and the where statement has to depend on a variable.
-- But since SQL doesn't let you create a view using a variable,
-- I have to do an 'eval'-style trick to create the view, concatting the create view statement
-- together with the value of the variable baked into it, then executing that statement.
--
-- In order to include the parameter this method is being used
-- https://stackoverflow.com/questions/11580134/prepare-statemnt-using-concat-in-mysql-giving-error

-- Query OK, 0 rows affected (0.00 sec)
CREATE OR REPLACE VIEW silverpop_export_view_full AS
  SELECT
    e.contact_id ContactID,
    c.email_greeting_display as email_greeting,
    e.contact_hash,
    e.email,
    IFNULL(e.first_name, '') firstname,
    IFNULL(e.last_name, '') lastname,
    COALESCE(pt.preference_tags, '') as preferences_tags,
    CASE
      WHEN gender_id =1 THEN 'Female'
      WHEN gender_id =2 THEN 'Male'
      WHEN gender_id =3 THEN 'Transgender'
      ELSE ''
    END as gender,
    IFNULL(country, 'XX') country,
    state,
    postal_code,
    e.employer_name,
    e.employer_id,
    SUBSTRING(e.preferred_language, 1, 2) IsoLang,
    COALESCE(donor_segment_id, 1000) as donor_segment_id,
    CASE
        WHEN donor_segment_id = 100 THEN 'Major Donor'
        WHEN donor_segment_id = 200 THEN 'Mid Tier'
        WHEN donor_segment_id = 200 THEN 'Mid-Value Prospect'
        WHEN donor_segment_id = 400 THEN 'Recurring donor'
        WHEN donor_segment_id = 500 THEN 'Grassroots Plus Donor'
        WHEN donor_segment_id = 600 THEN 'Grassroots Donor'
        WHEN donor_segment_id = 900 THEN 'All other Donors'
        WHEN donor_segment_id = 1000 THEN 'Non Donor'
        ELSE 'Non Donor'
        END as donor_segment,
    donor_status_id,
    CASE
        WHEN donor_status_id = 2 THEN 'Active Recurring'
        WHEN donor_status_id = 4 THEN 'Delinquent Recurring'
        WHEN donor_status_id = 6 THEN 'Recent lapsed Recurring'
        WHEN donor_status_id = 8 THEN 'Deep lapsed Recurring'
        WHEN donor_status_id = 20 THEN 'Consecutive'
        WHEN donor_status_id = 25 THEN 'New'
        WHEN donor_status_id = 30 THEN 'Active'
        WHEN donor_status_id = 35 THEN 'Lybunt'
        WHEN donor_status_id = 50 THEN 'Lapsed'
        WHEN donor_status_id = 60 THEN 'Deep Lapsed'
        WHEN donor_status_id = 70 THEN 'Ultra lapsed'
        WHEN donor_status_id = 1000 THEN 'Non Donor'
        ELSE 'Non Donor'
        END as donor_status,
    CASE WHEN opted_in IS NULL THEN '' ELSE IF(opted_in,'Yes','No') END AS latest_optin_response,
    IFNULL(DATE_FORMAT(birth_date, '%m/%d/%Y'), '') TS_birth_date,
    COALESCE(charitable_contributions_decile, '') as TS_charitable_contributions_decile,
    COALESCE(disc_income_decile, '') as TS_disc_income_decile,
    CASE
      WHEN estimated_net_worth_144 = '1' THEN'$20 Million +'
      WHEN estimated_net_worth_144 = '2' THEN '$10 Million - $19.99 Million'
      WHEN estimated_net_worth_144 = '3' THEN '$5 Million - $9.99 Million'
      WHEN estimated_net_worth_144 = '4' THEN '$2 Million - $4.99 Million'
      WHEN estimated_net_worth_144 = '5' THEN '$1 Million - $1.99 Million'
      WHEN estimated_net_worth_144 = '6' THEN '$500,000 - $999,999'
      WHEN estimated_net_worth_144 = '7' THEN '>$5B'
      WHEN estimated_net_worth_144 = '8' THEN '>$1B'
      WHEN estimated_net_worth_144 = '9' THEN '>$10B'
      WHEN estimated_net_worth_144 = '10' THEN '$100 Million +'
      WHEN estimated_net_worth_144 = 'A' THEN 'Below $25,000'
      WHEN estimated_net_worth_144 = 'B' THEN '$25,000 - $49,999'
      WHEN estimated_net_worth_144 = 'C' THEN '$50,000 - $74,999'
      WHEN estimated_net_worth_144 = 'D' THEN '$75,000 - $99,999'
      WHEN estimated_net_worth_144 = 'E' THEN '$150,000 - $199,999'
      WHEN estimated_net_worth_144 = 'F' THEN '$150,000 - $199,999'
      WHEN estimated_net_worth_144 = 'G' THEN '$200,000 - $249,999'
      WHEN estimated_net_worth_144 = 'H' THEN '$250,000 - $499,999'
      WHEN estimated_net_worth_144 = 'I' THEN '$500,000 - $749,999'
      WHEN estimated_net_worth_144 = 'J' THEN '$750,000 - $999,999'
      WHEN estimated_net_worth_144 = 'K' THEN '$1,000,000 - $2,499,999'
      WHEN estimated_net_worth_144 = 'L' THEN '$2,500,000 - $4,999,999'
      WHEN estimated_net_worth_144 = 'M' THEN '$5,000,000 - $9,999,999'
      WHEN estimated_net_worth_144 = 'N' THEN 'Above $10,000,000'
      ELSE ''
    END as TS_estimated_net_worth,
    CASE
      WHEN family_composition_173 = '1' THEN 'Single'
      WHEN family_composition_173 = '2' THEN 'Single with Children'
      WHEN family_composition_173 = '3' THEN 'Couple'
      WHEN family_composition_173 = '4' THEN 'Couple with children'
      WHEN family_composition_173 = '5' THEN 'Multiple Generations'
      WHEN family_composition_173 = '6' THEN 'Multiple Surnames (3+)'
      WHEN family_composition_173 = '7' THEN 'Other'
      ELSE ''
    END as TS_family_composition,
    CASE
      WHEN income_range = 'a' THEN 'Below $30,000'
      WHEN income_range = 'b' THEN '$30,000 - $39,999'
      WHEN income_range = 'c' THEN '$40,000 - $49,999'
      WHEN income_range = 'd' THEN '$50,000 - $59,999'
      WHEN income_range = 'e' THEN '$60,000 - $74,999'
      WHEN income_range = 'f' THEN '$75,000 - $99,999'
      WHEN income_range = 'g' THEN '$100,000 - $124,999'
      WHEN income_range = 'h' THEN '$125,000 - $149,999'
      WHEN income_range = 'i' THEN '$150,000 - $199,999'
      WHEN income_range = 'j' THEN '$200,000 - $249,999'
      WHEN income_range = 'k' THEN '$250,000 - $299,999'
      WHEN income_range = 'l' THEN '$300,000 - $499,999'
      WHEN income_range = 'm' THEN 'Above $500,000'
      ELSE ''
    END as TS_income_range,
    CASE
      WHEN occupation_175 = '1' THEN 'Professional/Technical'
      WHEN occupation_175 = '2' THEN 'Upper Management/Executive'
      WHEN occupation_175 = '3' THEN 'Sales/Service'
      WHEN occupation_175 = '4' THEN 'Office/Clerical'
      WHEN occupation_175 = '5' THEN 'Skilled Trade'
      WHEN occupation_175 = '6' THEN 'Retired'
      WHEN occupation_175 = '7' THEN 'Administrative/Management'
      WHEN occupation_175 = '8' THEN 'Self Employed'
      WHEN occupation_175 = '9' THEN 'Military'
      WHEN occupation_175 = '10' THEN 'Farming/Agriculture'
      WHEN occupation_175 = '11' THEN 'Medical/Health Services'
      WHEN occupation_175 = '12' THEN 'Financial Services'
      WHEN occupation_175 = '13' THEN 'Teacher/Educator'
      WHEN occupation_175 = '14' THEN 'Legal Services'
      WHEN occupation_175 = '15' THEN 'Religious'
      ELSE ''
    END as TS_occupation,
    '' as dataaxle_is_grandparent,
    '' as directmail_receivers,
    '' as directmail_id,
    -- These 2 fields have been coalesced further up so we know they have a value. Addition at this point is cheap.
    (donation_count + endowment_number_donations) as both_funds_donation_count,
    IFNULL(DATE_FORMAT(IF (endowment_first_donation_date IS NULL OR foundation_first_donation_date < endowment_first_donation_date , foundation_first_donation_date, endowment_first_donation_date), '%m/%d/%Y'), '')
      as both_funds_first_donation_date,
    IFNULL(DATE_FORMAT(IF (endowment_highest_usd_amount > foundation_highest_usd_amount, endowment_highest_donation_date, foundation_highest_donation_date), '%m/%d/%Y'), '')
      as both_funds_highest_donation_date,
    IF (endowment_highest_native_amount > foundation_highest_native_amount, endowment_highest_native_amount, foundation_highest_native_amount)
        as both_funds_highest_native_amount,
    IF (endowment_highest_usd_amount > foundation_highest_usd_amount, endowment_highest_usd_amount, foundation_highest_usd_amount)
      as both_funds_highest_usd_amount,
    IFNULL(DATE_FORMAT(IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_last_donation_date, endowment_last_donation_date), '%m/%d/%Y'), '')
      as both_funds_latest_donation_date,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_latest_native_amount, endowment_latest_native_amount)
     as both_funds_latest_native_amount,
    IFNULL(DATE_FORMAT(endowment_last_donation_date, '%m/%d/%Y'), '') as endowment_latest_donation_date,
    IFNULL(DATE_FORMAT(endowment_first_donation_date, '%m/%d/%Y'), '') as endowment_first_donation_date,
    endowment_number_donations as endowment_donation_count,
    IFNULL(DATE_FORMAT(endowment_highest_donation_date, '%m/%d/%Y'), '') as endowment_highest_donation_date,
    COALESCE(endowment_highest_native_amount, 0) as endowment_highest_native_amount,
    COALESCE(endowment_highest_native_currency, '') as endowment_highest_native_currency,
    COALESCE(endowment_highest_usd_amount, 0) as endowment_highest_usd_amount,
    COALESCE(endowment_latest_currency, '') as endowment_latest_currency,
    COALESCE(endowment_latest_native_amount,0) as endowment_latest_native_amount,
    donation_count as AF_donation_count,
    IFNULL(DATE_FORMAT(foundation_first_donation_date, '%m/%d/%Y'), '') as AF_first_donation_date,
    IFNULL(DATE_FORMAT(foundation_highest_donation_date, '%m/%d/%Y'), '') as AF_highest_donation_date,
    foundation_highest_usd_amount as AF_highest_usd_amount,
    IFNULL(DATE_FORMAT(foundation_last_donation_date, '%m/%d/%Y'), '') as AF_latest_donation_date,
    COALESCE(foundation_latest_native_amount, 0) as AF_latest_native_amount,
    foundation_highest_native_amount as AF_highest_native_amount,
    foundation_highest_native_currency as AF_highest_native_currency,
    lifetime_usd_total as AF_lifetime_usd_total,
    COALESCE(foundation_latest_currency, '') as AF_latest_currency,
    COALESCE(foundation_latest_currency_symbol, '') as AF_latest_currency_symbol,
    IF(foundation_has_recurred_donation, 'Yes', 'No') as AF_has_recurred_donation,
    IF(foundation_has_active_recurring_donation, 'Yes', 'No') as AF_has_active_recurring_donation,
    IFNULL(DATE_FORMAT(foundation_recurring_first_donation_date, '%m/%d/%Y'), '') as AF_recurring_first_donation_date,
    IFNULL(DATE_FORMAT(foundation_recurring_latest_donation_date, '%m/%d/%Y'), '') as AF_recurring_latest_donation_date,
    COALESCE(cr.amount, 0) as AF_recurring_latest_native_amount,
    COALESCE(cr.currency, '') as AF_recurring_latest_currency,
    IF (pp.name IN ('adyen', 'ingenico') AND foundation_recurring_active_count = 1 AND recurring_has_upgrade_activity = 0 AND cr.frequency_unit = 'month', 'Yes', 'No')
        as AF_recurring_eligible_for_upgrade,
    '' as both_funds_has_given_on_email,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_latest_currency, endowment_latest_currency)
     as both_funds_latest_currency,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_latest_currency_symbol, endowment_latest_currency_symbol)
     as both_funds_latest_currency_symbol,
    e.modified_date,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , COALESCE(latest_donation_source, ''), COALESCE(endowment_latest_donation_source, ''))
       as both_funds_latest_donation_source,
    '' as both_funds_latest_payment_method,
    all_funds_total_2018_2019 as both_funds_usd_total_fy1819,
    all_funds_total_2019_2020 as both_funds_usd_total_fy1920,
    all_funds_total_2020_2021 as both_funds_usd_total_fy2021,
    all_funds_total_2021_2022 as both_funds_usd_total_fy2122,
    all_funds_total_2022_2023 as both_funds_usd_total_fy2223,
    all_funds_total_2023_2024 as both_funds_usd_total_fy2324,
    all_funds_total_2024_2025 as both_funds_usd_total_fy2425,
    all_funds_total_2025_2026 as both_funds_usd_total_fy2526,
    IFNULL(gift.matching_gifts_provider_info_url, '') as matching_gifts_provider_info_url,
    IFNULL(gift.guide_url, '') matching_gifts_guide_url,
    IFNULL(gift.online_form_url, '') matching_gifts_online_form_url,
    IFNULL(most_recent_cancel_reason, '') most_recent_cancel_reason
  FROM (
    SELECT *,
    CASE
      WHEN donor_status_bin & 256 /* B'100000000' */ THEN 2
      WHEN donor_status_bin & 128 /* B'010000000' */ THEN 4
      WHEN donor_status_bin &  64 /* B'001000000' */ THEN 6
      WHEN donor_status_bin &  32 /* B'000100000' */ THEN 8
      -- Has a donation this year and one last year (and potentially others)
      WHEN donor_status_bin & 24 = 24 /* B'000011000' */ THEN 20
      -- Note exact match, just one donation this year and nothing else
      WHEN donor_status_bin =  16 /* B'000010000' */ THEN 25
      -- Has a donation this year (and something else, since above not matched)
      WHEN donor_status_bin &  16 /* B'000010000' */ THEN 30
      -- Has a donation last year (and maybe before). Not this year, since above not matched)
      WHEN donor_status_bin &   8 /* B'000001000' */ THEN 35
      -- Last donation two years ago
      WHEN donor_status_bin &   4 /* B'000000100' */ THEN 50
      -- Last donation up to 5 years ago
      WHEN donor_status_bin &   2 /* B'000000010' */ THEN 60
      WHEN donor_status_bin &   1 /* B'000000001' */ THEN 70
      ELSE 1000
  END as donor_status_id
  FROM silverpop_export) AS e
  LEFT JOIN civicrm.civicrm_value_1_prospect_5 v ON v.entity_id = contact_id
  LEFT JOIN civicrm.civicrm_contact c ON c.id = contact_id
  LEFT JOIN silverpop_endowment_latest endow_late ON endow_late.email = e.email
  LEFT JOIN silverpop_export_latest latest ON e.email = latest.email
  LEFT JOIN silverpop_endowment_highest endow_high ON endow_high.email = e.email
  LEFT JOIN preference_tags pt ON pt.email = e.email
  LEFT JOIN civicrm.civicrm_value_matching_gift gift ON gift.entity_id = e.employer_id
  LEFT JOIN civicrm.civicrm_contribution_recur cr ON e.foundation_recurring_latest_contribution_recur_id = cr.id
  LEFT JOIN cancel_reason ON cancel_reason.email = e.email
  LEFT JOIN civicrm.civicrm_payment_processor pp ON cr.payment_processor_id = pp.id;

SET @sql =CONCAT("CREATE OR REPLACE VIEW silverpop_export_view AS
SELECT ContactID,
IsoLang,

AF_donation_count,
AF_first_donation_date,
AF_has_active_recurring_donation,
AF_highest_donation_date,
AF_highest_native_amount,
AF_highest_native_currency,
AF_highest_usd_amount,
AF_latest_currency,
AF_latest_currency_symbol,
AF_latest_donation_date,
AF_latest_native_amount,
AF_lifetime_usd_total,
AF_recurring_first_donation_date,
AF_recurring_latest_donation_date,
AF_recurring_latest_native_amount,
AF_recurring_latest_currency,
AF_recurring_eligible_for_upgrade,
both_funds_donation_count,
both_funds_first_donation_date,
both_funds_has_given_on_email,
both_funds_highest_native_amount,
both_funds_highest_donation_date,
both_funds_highest_usd_amount,
both_funds_latest_currency,
both_funds_latest_currency_symbol,
both_funds_latest_donation_date,
both_funds_latest_donation_source,
both_funds_latest_native_amount,
both_funds_latest_payment_method,
both_funds_usd_total_fy1819,
both_funds_usd_total_fy1920,
both_funds_usd_total_fy2021,
both_funds_usd_total_fy2122,
both_funds_usd_total_fy2223,
both_funds_usd_total_fy2324,
both_funds_usd_total_fy2425,
both_funds_usd_total_fy2526,
contact_hash,
country,
dataaxle_is_grandparent,
directmail_receivers,
directmail_id,
donor_segment,
donor_segment_id,
donor_status,
donor_status_id,
email,
email_greeting,
employer_id,
employer_name,
matching_gifts_provider_info_url,
matching_gifts_guide_url,
matching_gifts_online_form_url,
endowment_donation_count,
endowment_first_donation_date,
endowment_highest_donation_date,
endowment_highest_native_amount,
endowment_highest_native_currency,
endowment_highest_usd_amount,
endowment_latest_donation_date,
endowment_latest_currency,
endowment_latest_native_amount,
firstname,
gender,
lastname,
latest_optin_response,
most_recent_cancel_reason,
postal_code,
preferences_tags,
state,
TS_birth_date,
TS_charitable_contributions_decile,
TS_disc_income_decile,
TS_estimated_net_worth,
TS_family_composition,
TS_income_range,
TS_occupation
FROM silverpop_export_view_full
WHERE modified_date > DATE_SUB(NOW(), INTERVAL ", @offSetInDays, " DAY)");
prepare stmnt1 from @sql;
execute stmnt1;
deallocate prepare stmnt1;
