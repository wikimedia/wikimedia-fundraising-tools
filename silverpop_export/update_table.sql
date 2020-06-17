-- Updates the silverpop_export table
--
-- TODO: Most of the complexity will go away once our contacts' exact email
-- matches have been deduped.
--
-- Timing is from a 2019-08-22 Staging test.

SET autocommit = 1;

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
-- (16 min 25.15 sec)
INSERT INTO silverpop_export_staging
  (id, modified_date, contact_id, contact_hash, email, first_name, last_name, preferred_language, opted_out, opted_in)
  SELECT
    e.id,
    c.modified_date,
    e.contact_id, c.hash, e.email, c.first_name, c.last_name,
    REPLACE(c.preferred_language, '_', '-'),
    (c.is_opt_out OR c.do_not_email OR e.on_hold OR COALESCE(v.do_not_solicit, 0)),
    v.opt_in
  FROM civicrm.civicrm_email e
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id
  LEFT JOIN civicrm.civicrm_value_1_communication_4 v ON v.entity_id = c.id
  WHERE
    e.email IS NOT NULL AND e.email != ''
    AND c.is_deleted = 0
    AND e.is_primary = 1;

-- Collect email addresses which should be excluded for various reasons, such as:
-- * Exclude non-primary addresses
-- * Exclude any "former residence" email addresses.
-- * Exclude addresses dropped during contact merge.
-- We grab ALL addresses from the logs to start, then after we've figured out
-- which of the addresses on the include list are good, we remove them from
-- this table.
-- Same no-op update trick as with silverpop_export_latest
-- 44 min 38.01 sec
INSERT INTO silverpop_excluded (email)
  SELECT email
    FROM log_civicrm.log_civicrm_email e
   -- Ignore addresses created after the last address we picked
   -- up in the staging table select query above, so we don't
   -- opt-out contacts created since then.
   WHERE id <= (SELECT MAX(id) FROM silverpop_export_staging)
ON DUPLICATE KEY UPDATE email = silverpop_excluded.email;

-- Find the latest donation for each email address. Ordering by
-- receive_date and total_amount descending should always insert
-- the latest donation first, with the larger prevailing for an
-- email with multiple simultaneous donations. All the rest for
-- that email will be ignored due to the unique constraint. We
-- use 'ON DUPLICATE KEY UPDATE' instead of 'INSERT IGNORE' as
-- the latter throws warnings.
-- (8 min 6.67 sec)
INSERT INTO silverpop_export_latest
  SELECT
    e.email,
    d.last_donation_currency,
    COALESCE(cur.symbol, d.last_donation_currency),
    d.last_donation_amount,
    d.last_donation_date
  FROM
    silverpop_export_staging e
    INNER JOIN civicrm.wmf_donor d ON d.entity_id = e.contact_id
    LEFT JOIN civicrm.civicrm_currency cur
      ON cur.name = d.last_donation_currency
  WHERE
    d.last_donation_date IS NOT NULL
  ORDER BY last_donation_date DESC, d.last_donation_usd DESC
ON DUPLICATE KEY UPDATE latest_currency = silverpop_export_latest.latest_currency;

-- Populate table for highest donation amount and date
-- (18 min 13.39 sec)
INSERT INTO silverpop_export_highest
  SELECT
    e.email,
    ex.original_currency,
    ex.original_amount,
    ct.total_amount,
    ct.receive_date
  FROM
    silverpop_export_staging e,
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

CREATE TABLE silverpop_export_dedupe_email (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email varchar(255),
  maxid int,
  preferred_language varchar(12),
  opted_out tinyint(1),

  INDEX spexde_email (email)
) COLLATE 'utf8_unicode_ci';

-- Deduplicate rows that have the same email address.
-- We will have to merge in more data later, but this is ~1.5M rows we're
-- getting rid of here which is more better than taking them all the way
-- through.
-- 1 min 31.96 sec
INSERT INTO silverpop_export_dedupe_email (email, maxid, opted_out)
   SELECT email, max(id) maxid, max(opted_out) opted_out
     FROM silverpop_export_staging
       FORCE INDEX (spex_email)
       GROUP BY email
       HAVING count(*) > 1;

-- We pull in language from the parent table so that we
-- can preserve it and not propagate nulls
-- 30.85 sec
UPDATE silverpop_export_dedupe_email exde, silverpop_export_staging ex
  SET
    exde.preferred_language = ex.preferred_language
  WHERE
    ex.email = exde.email AND
    ex.preferred_language IS NOT NULL;

-- Delete duplicated email addresses from the main staging table
-- (1 min 2.15 sec)
DELETE silverpop_export_staging FROM silverpop_export_staging, silverpop_export_dedupe_email
  WHERE
    silverpop_export_staging.email = silverpop_export_dedupe_email.email AND
    silverpop_export_staging.id != silverpop_export_dedupe_email.maxid;

-- Make sure the remaining rows all have opt-out and language set correctly
-- (18.34 sec)
UPDATE silverpop_export_staging ex, silverpop_export_dedupe_email exde
  SET
    ex.opted_out = exde.opted_out,
    ex.preferred_language = exde.preferred_language
  WHERE
    exde.maxid = ex.id;

-- Populate the aggregate table from a full contribution table scan
-- 28 min 41.38 sec
INSERT INTO silverpop_export_stat
  (email, exid, total_usd, cnt_total, first_donation_date,
   total_2014, total_2015, total_2016, total_2017,
   total_2018, total_2019, total_2020,
   endowment_last_donation_date, endowment_first_donation_date, endowment_number_donations
  )
  SELECT
    e.email,
    MAX(ex.id),
    COALESCE(SUM(donor.lifetime_usd_total), 0) as lifetime_usd_total,
    COALESCE(SUM(donor.number_donations), 0) as number_donations,
    MIN(donor.first_donation_date) as first_donation_date,
    COALESCE(SUM(donor.total_2014), 0) as total_2014,
    COALESCE(SUM(donor.total_2015), 0) as total_2015,
    COALESCE(SUM(donor.total_2016), 0) as total_2016,
    COALESCE(SUM(donor.total_2017), 0) as total_2017,
    COALESCE(SUM(donor.total_2018), 0) as total_2018,
    COALESCE(SUM(donor.total_2019), 0) as total_2019,
    COALESCE(SUM(donor.total_2020), 0) as total_2020,
    MAX(donor.endowment_last_donation_date) as endowment_last_donation_date,
    MIN(donor.endowment_first_donation_date) as endowment_first_donation_date,
    COALESCE(SUM(donor.endowment_number_donations), 0) as endowment_number_donations
  FROM civicrm.civicrm_email e FORCE INDEX(UI_email)
  JOIN silverpop_export_staging ex ON e.email=ex.email
  LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = e.contact_id
  # We need to be careful with this group by. We want the sum by email but we don't want
  # any other left joins that could be 1 to many & inflate the aggregates.
  GROUP BY e.email;

-- Mark all emails associated with a recurring donations
-- 1 min 31.95 sec
UPDATE
  civicrm.civicrm_contribution_recur recur
  INNER JOIN civicrm.civicrm_contribution contributions
    ON recur.id = contributions.contribution_recur_id
    AND contributions.contribution_status_id = 1
    AND contributions.financial_type_id != 26
    AND contributions.total_amount > 0
  INNER JOIN civicrm.civicrm_email email ON recur.contact_id = email.contact_id
  INNER JOIN silverpop_export_stat stat ON stat.email = email.email
  SET has_recurred_donation = 1;

-- Get latest postal address for each email.
-- (16 minutes)
INSERT INTO silverpop_export_address
SELECT      e.email, a.city, ctry.iso_code, st.name, a.postal_code
  FROM      civicrm.civicrm_email e
  JOIN      silverpop_export_staging ex
    ON      e.email = ex.email
  JOIN      civicrm.civicrm_address a
    ON      e.contact_id = a.contact_id AND a.is_primary = 1
  JOIN      civicrm.civicrm_country ctry
    ON      a.country_id = ctry.id
  LEFT JOIN civicrm.civicrm_state_province st
    ON      a.state_province_id = st.id
  WHERE     ex.opted_out = 0
  ORDER BY  a.id DESC
ON DUPLICATE KEY UPDATE email = e.email;

-- Pull in address and latest/greatest/cumulative stats from intermediate tables
UPDATE silverpop_export_staging ex
  LEFT JOIN silverpop_export_stat exs ON ex.id = exs.exid
  LEFT JOIN silverpop_export_latest lt ON ex.email = lt.email
  LEFT JOIN silverpop_export_highest hg ON ex.email = hg.email
  LEFT JOIN silverpop_export_address addr ON ex.email = addr.email
  SET
    ex.lifetime_usd_total = COALESCE(exs.total_usd, 0),
    ex.total_2014 = exs.total_2014,
    ex.total_2015 = exs.total_2015,
    ex.total_2016 = exs.total_2016,
    ex.total_2017 = exs.total_2017,
    ex.total_2018 = exs.total_2018,
    ex.total_2019 = exs.total_2019,
    ex.total_2020 = exs.total_2020,
    ex.endowment_last_donation_date = exs.endowment_last_donation_date,
    ex.endowment_first_donation_date = exs.endowment_first_donation_date,
    ex.endowment_number_donations = exs.endowment_number_donations,
    ex.donation_count = exs.cnt_total,
    ex.donation_count = COALESCE(exs.cnt_total, 0),
    ex.has_recurred_donation = COALESCE(exs.has_recurred_donation, 0),
    ex.first_donation_date = exs.first_donation_date,
    ex.latest_currency = COALESCE(lt.latest_currency, ''),
    ex.latest_currency_symbol = COALESCE(lt.latest_currency_symbol, ''),
    ex.latest_native_amount = COALESCE(lt.latest_native_amount, 0),
    ex.latest_donation = lt.latest_donation,
    ex.highest_native_currency = COALESCE(hg.highest_native_currency, ''),
    ex.highest_native_amount = COALESCE(hg.highest_native_amount, 0),
    ex.highest_usd_amount = COALESCE(hg.highest_usd_amount, 0),
    ex.highest_donation_date = hg.highest_donation_date,
    ex.city = addr.city,
    ex.country = addr.country,
    ex.postal_code = addr.postal_code,
    ex.state = addr.state;

-- Fill in missing countries from contribution_tracking
-- (15 minutes)
UPDATE
    silverpop_export_staging ex,
    civicrm.civicrm_contribution ct,
    drupal.contribution_tracking dct
  SET
    ex.country = dct.country
  WHERE
    ex.country IS NULL AND
    ex.contact_id = ct.contact_id AND
    dct.contribution_id = ct.id AND
    dct.country IS NOT NULL AND
    ex.opted_out = 0;

-- Reconstruct the donors likely language from their country if it
-- exists from a table of major language to country.
UPDATE silverpop_export_staging ex, silverpop_countrylangs cl
  SET ex.preferred_language = cl.lang
  WHERE
    ex.country IS NOT NULL AND
    ex.preferred_language IS NULL AND
    ex.country = cl.country AND
    ex.opted_out = 0;

-- Still no language? Default 'em to English
UPDATE silverpop_export_staging SET preferred_language='en' WHERE preferred_language IS NULL;

-- Remove all the known-good addresses from the suppression list.
DELETE silverpop_excluded
  FROM silverpop_excluded
  JOIN silverpop_export_staging s
    ON s.email = silverpop_excluded.email
    WHERE s.opted_out = 0
    AND (s.opted_in IS NULL OR s.opted_in = 1);

-- We don't want to suppress emails of Civi users.
-- Conveniently, the account name is the email address in
-- in the table that associates contacts with accounts.
DELETE silverpop_excluded
  FROM silverpop_excluded
  JOIN civicrm.civicrm_uf_match m
    ON m.uf_name = silverpop_excluded.email;

-- Move the data from the staging table into the persistent one
-- (12 minutes)
INSERT INTO silverpop_export (
  id,contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in,
  has_recurred_donation,highest_usd_amount,highest_native_amount,
  highest_native_currency,highest_donation_date,lifetime_usd_total,donation_count,
  latest_currency,latest_currency_symbol,latest_native_amount,
  latest_donation, first_donation_date,city,country,state,postal_code,
  total_2014, total_2015, total_2016, total_2017,
  total_2018, total_2019, total_2020, endowment_last_donation_date, endowment_first_donation_date, endowment_number_donations)
SELECT id,contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in,
  has_recurred_donation,highest_usd_amount,highest_native_amount,
  highest_native_currency,highest_donation_date,lifetime_usd_total,donation_count,
  latest_currency,latest_currency_symbol,latest_native_amount,
  latest_donation,first_donation_date,city,country,state,postal_code,
  total_2014, total_2015, total_2016, total_2017,
  total_2018, total_2019, total_2020, endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations
FROM silverpop_export_staging
WHERE opted_out=0
AND (opted_in IS NULL OR opted_in = 1)

ON DUPLICATE KEY UPDATE email = silverpop_export.email;

-- Create a nice view to export from
CREATE OR REPLACE VIEW silverpop_export_view AS
  SELECT
    contact_id ContactID,
    e.contact_hash,
    email,
    IFNULL(e.first_name, '') firstname,
    IFNULL(e.last_name, '') lastname,
    IFNULL(country, 'XX') country,
    state,
    postal_code,
    SUBSTRING(e.preferred_language, 1, 2) IsoLang,
    IF(has_recurred_donation, 'YES', 'NO') has_recurred_donation,
    CASE WHEN opted_in IS NULL THEN '' ELSE IF(opted_in,'YES','NO') END AS latest_optin_response,
    highest_usd_amount,
    highest_native_amount,
    highest_native_currency,
    IFNULL(DATE_FORMAT(highest_donation_date, '%m/%d/%Y'), '') highest_donation_date,
    lifetime_usd_total,
    IFNULL(DATE_FORMAT(latest_donation, '%m/%d/%Y'), '') latest_donation_date,
    latest_currency,
    latest_currency_symbol,
    latest_native_amount,
    donation_count,
    IFNULL(DATE_FORMAT(first_donation_date, '%m/%d/%Y'), '') first_donation_date,
    total_2014,
    total_2015,
    total_2016,
    total_2017,
    total_2018,
    total_2019,
    total_2020,
    IFNULL(DATE_FORMAT(endowment_last_donation_date, '%m/%d/%Y'), '') endowment_last_donation_date,
    IFNULL(DATE_FORMAT(endowment_first_donation_date, '%m/%d/%Y'), '') endowment_first_donation_date,
    endowment_number_donations,
    CASE
        WHEN family_composition_173 = '1' THEN 'Single'
        WHEN family_composition_173 = '2' THEN 'Single with Children'
        WHEN family_composition_173 = '3' THEN 'Couple'
        WHEN family_composition_173 = '4' THEN 'Couple with children'
        WHEN family_composition_173 = '5' THEN 'Multiple Generations'
        WHEN family_composition_173 = '6' THEN 'Multiple Surnames (3+)'
        WHEN family_composition_173 = '7' THEN 'Other'
        ELSE ''
    END as z_family_composition,
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
    END as z_estimated_net_worth,
    COALESCE(charitable_contributions_decile, '') as z_charitable_contributions_decile,
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
     END as z_voter_party,
    COALESCE(disc_income_decile, '') as z_disc_income_decile,
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
    END as z_occupation,
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
    END as z_income_range,
    CASE
        WHEN gender_id =1 THEN 'Female'
        WHEN gender_id =2 THEN 'Male'
        WHEN gender_id =3 THEN 'Transgender'
        ELSE ''
    END as z_gender,
    IFNULL(DATE_FORMAT(birth_date, '%m/%d/%Y'), '') z_birth_date

  FROM silverpop_export e
  LEFT JOIN civicrm.civicrm_value_1_prospect_5 v ON v.entity_id = contact_id
  LEFT JOIN civicrm.civicrm_contact c ON c.id = contact_id;
