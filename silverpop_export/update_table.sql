SET autocommit = 1;
INSERT INTO silverpop_export_matching_gift
(employer_id, employer_name, matching_gifts_provider_info_url, guide_url, online_form_url, minimum_gift_matched_usd, match_policy_last_updated)
SELECT
    entity_id,
    name_from_matching_gift_db,
    matching_gifts_provider_info_url,
    guide_url,
    online_form_url,
    minimum_gift_matched_usd,
    match_policy_last_updated
FROM
    civicrm.civicrm_value_matching_gift;

-- Updates the silverpop_export table

-- Explanation of tables (as of now, still being re-worked).
-- silverpop_export_staging - summarised contact data with complexities around country, language, opt in, opt out resolved
-- silverpop_missing_countries - support table for building the above table
-- silverpop_email_map - summary table of contact data where we want 'the one that has this data', provides master_id
--    for later filtering. Note master_id currently calculated by highest email id - ideally would be most recent donor
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
  INSERT INTO silverpop_export_stat
  (email,
   all_funds_latest_donation_date,
   foundation_lifetime_usd_total,
   foundation_donation_count, foundation_first_donation_date,
   foundation_last_donation_date,
   foundation_highest_usd_amount,
   endowment_highest_usd_amount,
   foundation_total_2014, foundation_total_2015, foundation_total_2016, foundation_total_2017,
   foundation_total_2018, foundation_total_2019, foundation_total_2020,
   endowment_last_donation_date, endowment_first_donation_date, endowment_number_donations
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
    COALESCE(SUM(donor.total_2014), 0) as foundation_total_2014,
    COALESCE(SUM(donor.total_2015), 0) as foundation_total_2015,
    COALESCE(SUM(donor.total_2016), 0) as foundation_total_2016,
    COALESCE(SUM(donor.total_2017), 0) as foundation_total_2017,
    COALESCE(SUM(donor.total_2018), 0) as foundation_total_2018,
    COALESCE(SUM(donor.total_2019), 0) as foundation_total_2019,
    COALESCE(SUM(donor.total_2020), 0) as foundation_total_2020,
    MAX(donor.endowment_last_donation_date) as endowment_last_donation_date,
    MIN(donor.endowment_first_donation_date) as endowment_first_donation_date,
    COALESCE(SUM(donor.endowment_number_donations), 0) as endowment_number_donations
  FROM silverpop_update_world t
    INNER JOIN civicrm.civicrm_email e FORCE INDEX(UI_email) ON e.email = t.email
      AND e.is_primary = 1
    LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = e.contact_id
    # We need to be careful with this group by. We want the sum by email but we don't want
    # any other left joins that could be 1 to many & inflate the aggregates.
  GROUP BY e.email;

COMMIT;


-- Query OK, 23199001 rows affected (11 min 55.19 sec)
INSERT INTO silverpop_email_map
  SELECT email,
    # MAX here is attempt to get the most recent, although it would be better to accurately calculate most recent donor.
    MAX(id) as master_email_id,
    # We definitely prefer 'an' address over no address so use MAX - but ideally we would prefer most recent donor.
    MAX(address_id) as address_id,
    # Use MAX to prefer non-blank
    MAX(preferred_language) as preferred_language,
    # Use MAX as any opted out IS opted out.
    MAX(opted_out) as opted_out,
    # 0 if they have ever actually opted out, else 1
    # we use this for filtering so don't need to preserve the nuance.
    # This should be revisited per https://phabricator.wikimedia.org/T256522
    MIN(IF (opted_in = 0, 0, 1)) as opted_in,
    MAX(modified_date) as modified_date
  FROM silverpop_export_staging
    -- This index force seems to not change the speed much....
    FORCE INDEX (spex_email)
  GROUP BY email
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
INSERT INTO silverpop_export_latest
  -- temporarily specify the fields here as we no longer use latest_donation from this table
  -- and it may not be dropped on the target db yet.
  (email, latest_currency, latest_currency_symbol, latest_native_amount)
  SELECT
    t.email,
    MAX(extra.original_currency) as latest_currency,
    MAX(cur.symbol) as latest_currency_symbol,
    MAX(extra.original_amount) as latest_native_amount
  FROM silverpop_update_world t
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
COMMIT;


-- Populate table for highest donation amount and date
BEGIN;
-- Delete recent rows from highest table (make way for updated version).
-- Query OK, 679293 rows affected (4.27 sec)
DELETE highest FROM silverpop_update_world t INNER JOIN silverpop_export_highest highest ON t.email = highest.email;
-- Add recent rows to highest export table
-- Query OK, 679293 rows affected, 12 warnings (1 min 15.22 sec)
INSERT INTO silverpop_export_highest
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
INSERT INTO silverpop_endowment_latest
SELECT
  email.email,
  -- really we want the currency/amount associated with the highest amount on
  -- the highest date but the chances of 2 concurrent donations
  -- with different currencies are negligible
  -- so the value of handling currency better here is low.
  MAX(extra.original_currency) as endowment_latest_currency,
  MAX(cur.symbol) as endowment_latest_currency_symbol,
  MAX(extra.original_amount) as endowment_latest_native_amount
FROM silverpop_update_world t
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
COMMIT;

BEGIN;
-- Delete recent rows from endowment_highest table (make way for updated version).
-- Query OK, 73565 rows affected (0.51 sec)
DELETE highest FROM silverpop_update_world t INNER JOIN silverpop_endowment_highest highest ON t.email = highest.email;
-- Add recent rows to endowment_highest table
-- Query OK, 73565 rows affected (47.86 sec)
INSERT INTO silverpop_endowment_highest
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
  LEFT JOIN civicrm.civicrm_contribution c FORCE INDEX(received_date) ON  c.contact_id = email.contact_id
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
-- Query OK, 94904 rows affected (10.15 sec)
INSERT INTO silverpop_has_recur
 SELECT DISTINCT email.email, 1 as has_recurred_donation
 FROM
   civicrm.civicrm_contribution_recur recur
 INNER JOIN civicrm.civicrm_contribution contributions
   ON recur.id = contributions.contribution_recur_id
   AND contributions.contribution_status_id = 1
   AND contributions.financial_type_id != 26
   AND contributions.total_amount > 0
 INNER JOIN civicrm.civicrm_email email ON recur.contact_id = email.contact_id AND is_primary = 1
 INNER JOIN silverpop_update_world t ON t.email = email.email;
COMMIT;

BEGIN;
-- Delete recent rows from export table (make way for updated version).
-- Query OK, 653187 rows affected (10.02 sec)
DELETE export FROM silverpop_update_world t INNER JOIN silverpop_export export ON t.email = export.email;

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
  foundation_has_recurred_donation,
  foundation_highest_usd_amount,foundation_highest_native_amount,
  foundation_highest_native_currency,foundation_highest_donation_date,lifetime_usd_total,donation_count,
  foundation_latest_currency,foundation_latest_currency_symbol,foundation_latest_native_amount,
  foundation_last_donation_date, foundation_first_donation_date,
  city,country,state,postal_code,
  foundation_total_2014, foundation_total_2015, foundation_total_2016, foundation_total_2017,
  foundation_total_2018, foundation_total_2019, foundation_total_2020,
  endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations, endowment_highest_usd_amount
)
SELECT ex.id, dedupe_table.modified_date, ex.contact_id,ex.contact_hash,ex.first_name,ex.last_name,
  -- get the one associated with the master email, failing that 'any'
  COALESCE(ex.preferred_language, dedupe_table.preferred_language) as preferred_language,
  ex.email,ex.opted_in, ex.employer_id, ex.employer_name,
  foundation_has_recurred_donation,
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
  foundation_total_2014, foundation_total_2015, foundation_total_2016, foundation_total_2017,
  foundation_total_2018, foundation_total_2019, foundation_total_2020,
  endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations,
  COALESCE(endowment_highest_usd_amount,0) as endowment_highest_usd_amount
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

COMMIT;
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
    contact_id ContactID,
    e.contact_hash,
    e.email,
    IFNULL(e.first_name, '') firstname,
    IFNULL(e.last_name, '') lastname,
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
    CASE WHEN opted_in IS NULL THEN '' ELSE IF(opted_in,'YES','NO') END AS latest_optin_response,
    IFNULL(DATE_FORMAT(birth_date, '%m/%d/%Y'), '') prospect_birth_date,
    COALESCE(charitable_contributions_decile, '') as prospect_charitable_contributions_decile,
    COALESCE(disc_income_decile, '') as prospect_disc_income_decile,
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
    END as prospect_estimated_net_worth,
    CASE
      WHEN family_composition_173 = '1' THEN 'Single'
      WHEN family_composition_173 = '2' THEN 'Single with Children'
      WHEN family_composition_173 = '3' THEN 'Couple'
      WHEN family_composition_173 = '4' THEN 'Couple with children'
      WHEN family_composition_173 = '5' THEN 'Multiple Generations'
      WHEN family_composition_173 = '6' THEN 'Multiple Surnames (3+)'
      WHEN family_composition_173 = '7' THEN 'Other'
      ELSE ''
    END as prospect_family_composition,
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
    END as prospect_income_range,
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
    END as prospect_occupation,

    CASE
      WHEN voter_party = 'democrat' THEN 'Democrat'
      WHEN voter_party = 'republican' THEN 'Republican'
      WHEN voter_party = 'green' THEN 'Green'
      WHEN voter_party = 'independent' THEN 'Independent'
      WHEN voter_party = 'libertarian' THEN 'Libertarian'
      WHEN voter_party = 'no_party' THEN 'No Party'
      WHEN voter_party = 'other' THEN 'Other'
      WHEN voter_party = 'unaffiliated' THEN 'Unaffiliated'
      WHEN voter_party =  'unregistered' THEN 'Unregistered'
      WHEN voter_party = 'working_fam' THEN 'Working Fam'
      WHEN voter_party = 'conservative' THEN 'Conservative'
      ELSE ''
    END as prospect_party,
    -- These 2 fields have been coalesced further up so we know they have a value. Addition at this point is cheap.
    (donation_count + endowment_number_donations) as all_funds_donation_count,
    IFNULL(DATE_FORMAT(IF (endowment_first_donation_date IS NULL OR foundation_first_donation_date < endowment_first_donation_date , foundation_first_donation_date, endowment_first_donation_date), '%m/%d/%Y'), '')
      as all_funds_first_donation_date,
    IFNULL(DATE_FORMAT(IF (endowment_highest_usd_amount > foundation_highest_usd_amount, endowment_highest_donation_date, foundation_highest_donation_date), '%m/%d/%Y'), '')
      as all_funds_highest_donation_date,
    IF (endowment_highest_usd_amount > foundation_highest_usd_amount, endowment_highest_usd_amount, foundation_highest_usd_amount)
      as all_funds_highest_usd_amount,
    IFNULL(DATE_FORMAT(IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_last_donation_date, endowment_last_donation_date), '%m/%d/%Y'), '')
      as all_funds_latest_donation_date,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_latest_native_amount, endowment_latest_native_amount)
     as all_funds_latest_native_amount,
    IFNULL(DATE_FORMAT(endowment_last_donation_date, '%m/%d/%Y'), '') endowment_last_donation_date,
    IFNULL(DATE_FORMAT(endowment_first_donation_date, '%m/%d/%Y'), '') endowment_first_donation_date,
    endowment_number_donations,
    IFNULL(DATE_FORMAT(endowment_highest_donation_date, '%m/%d/%Y'), '') endowment_highest_donation_date,
    endowment_highest_native_amount,
    endowment_highest_native_currency,
    endowment_highest_usd_amount,
    endowment_latest_currency,
    endowment_latest_native_amount,
    donation_count as foundation_donation_count,
    IFNULL(DATE_FORMAT(foundation_first_donation_date, '%m/%d/%Y'), '') foundation_first_donation_date,
    IFNULL(DATE_FORMAT(foundation_highest_donation_date, '%m/%d/%Y'), '') foundation_highest_donation_date,
    foundation_highest_usd_amount as foundation_highest_usd_amount,
    IFNULL(DATE_FORMAT(foundation_last_donation_date, '%m/%d/%Y'), '') foundation_latest_donation_date,
    COALESCE(foundation_latest_native_amount, 0) as foundation_latest_native_amount,
    foundation_highest_native_amount,
    foundation_highest_native_currency,
    lifetime_usd_total as foundation_lifetime_usd_total,
    COALESCE(foundation_latest_currency, '') as foundation_latest_currency,
    COALESCE(foundation_latest_currency_symbol, '') as foundation_latest_currency_symbol,
    IF(foundation_has_recurred_donation, 'YES', 'NO') as foundation_has_recurred_donation,
    foundation_total_2014 as foundation_total_2014,
    foundation_total_2015 as foundation_total_2015,
    foundation_total_2016 as foundation_total_2016,
    foundation_total_2017 as foundation_total_2017,
    foundation_total_2018 as foundation_total_2018,
    foundation_total_2019 as foundation_total_2019,
    foundation_total_2020 as foundation_total_2020,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_latest_currency, endowment_latest_currency)
     as all_funds_latest_currency,
    IF (endowment_last_donation_date IS NULL OR foundation_last_donation_date > endowment_last_donation_date , foundation_latest_currency_symbol, endowment_latest_currency_symbol)
     as all_funds_latest_currency_symbol,
    e.modified_date,
    IFNULL(gift.matching_gifts_provider_info_url, '') matching_gifts_provider_info_url,
    IFNULL(gift.guide_url, '') matching_gifts_guide_url,
    IFNULL(gift.online_form_url, '') matching_gifts_online_form_url
  FROM silverpop_export e
  LEFT JOIN civicrm.civicrm_value_1_prospect_5 v ON v.entity_id = contact_id
  LEFT JOIN civicrm.civicrm_contact c ON c.id = contact_id
  LEFT JOIN silverpop_endowment_latest endow_late ON endow_late.email = e.email
  LEFT JOIN silverpop_endowment_highest endow_high ON endow_high.email = e.email
  LEFT JOIN civicrm.civicrm_value_matching_gift gift ON gift.entity_id = e.employer_id
;

SET @sql =CONCAT("CREATE OR REPLACE VIEW silverpop_export_view AS
SELECT ContactID,
IsoLang,
all_funds_donation_count,
all_funds_first_donation_date,
all_funds_highest_donation_date,
all_funds_highest_usd_amount,
all_funds_latest_currency,
all_funds_latest_currency_symbol,
all_funds_latest_donation_date,
all_funds_latest_native_amount,
contact_hash,
country,
email,
employer_id,
employer_name,
matching_gifts_provider_info_url,
matching_gifts_guide_url,
matching_gifts_online_form_url,
endowment_first_donation_date,
endowment_highest_donation_date,
endowment_highest_native_amount,
endowment_highest_native_currency,
endowment_highest_usd_amount,
endowment_last_donation_date,
endowment_latest_currency,
endowment_latest_native_amount,
endowment_number_donations,
firstname,
foundation_donation_count,
foundation_first_donation_date,
foundation_has_recurred_donation,
foundation_highest_donation_date,
foundation_highest_native_amount,
foundation_highest_native_currency,
foundation_highest_usd_amount,
foundation_latest_currency,
foundation_latest_currency_symbol,
foundation_latest_donation_date,
foundation_latest_native_amount,
foundation_lifetime_usd_total,
foundation_total_2014,
foundation_total_2015,
foundation_total_2016,
foundation_total_2017,
foundation_total_2018,
foundation_total_2019,
foundation_total_2020,
gender,
lastname,
latest_optin_response,
postal_code,
prospect_birth_date,
prospect_charitable_contributions_decile,
prospect_disc_income_decile,
prospect_estimated_net_worth,
prospect_family_composition,
prospect_income_range,
prospect_occupation,
prospect_party,
state
FROM silverpop_export_view_full
WHERE modified_date > DATE_SUB(NOW(), INTERVAL ", @offSetInDays, " DAY)");
prepare stmnt1 from @sql;
execute stmnt1;
deallocate prepare stmnt1;
