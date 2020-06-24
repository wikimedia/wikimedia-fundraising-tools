INSERT INTO silverpop_export_matching_gift
(id, name, matching_gifts_provider_info_url, guide_url, online_form_url, minimum_gift_matched_usd, match_policy_last_updated, subsidiaries)
SELECT
    id,
    name_from_matching_gift_db,
    matching_gifts_provider_info_url,
    guide_url,
    online_form_url,
    minimum_gift_matched_usd,
    match_policy_last_updated,
    subsidiaries
FROM
    civicrm.civicrm_value_matching_gift;

-- Updates the silverpop_export table

SET autocommit = 1;

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
-- 24 June 2020 Query OK, 23988864 rows affected (19 min 41.23 sec)
INSERT INTO silverpop_export_staging
  (id, modified_date, contact_id, contact_hash, email, first_name, last_name, preferred_language, opted_out, opted_in,
   employer_id, employer_name, address_id, city, postal_code, country, state)
  SELECT
    e.id,
    c.modified_date,
    e.contact_id, c.hash, e.email, c.first_name, c.last_name,
    REPLACE(c.preferred_language, '_', '-') as preferred_language,
    (c.is_opt_out OR c.do_not_email OR e.on_hold OR COALESCE(v.do_not_solicit, 0)) as opted_out,
    v.opt_in as opted_in,
    c.employer_id,
    IF(c.employer_id, c.organization_name, '') as employer_name,
    a.id as address_id,
    a.city,
    a.postal_code,
    ctry.iso_code as country,
    st.name as state
  FROM civicrm.civicrm_email e
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id
  LEFT JOIN civicrm.civicrm_value_1_communication_4 v ON v.entity_id = c.id
  LEFT JOIN civicrm.civicrm_address a ON a.contact_id = e.contact_id AND a.is_primary = 1
  LEFT JOIN civicrm.civicrm_country ctry
            ON a.country_id = ctry.id
  LEFT JOIN civicrm.civicrm_state_province st
            ON a.state_province_id = st.id
  WHERE
    e.email IS NOT NULL AND e.email != ''
    AND c.is_deleted = 0
    AND e.is_primary = 1;

-- Query OK, 23200800 rows affected (9 min 22.15 sec)
INSERT INTO silverpop_email_map
  SELECT email, MAX(id) as master_email_id, MAX(address_id) as address_id,
    MAX(preferred_language) as preferred_language,
    MAX(opted_out) as opted_out
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
-- Query OK, 19162022 rows affected, 11 warnings (8 min 55.71 sec)
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


-- Pull in address and latest/greatest/cumulative stats from intermediate tables
UPDATE silverpop_export_staging ex
  LEFT JOIN silverpop_export_stat exs ON ex.id = exs.exid
  LEFT JOIN silverpop_export_latest lt ON ex.email = lt.email
  LEFT JOIN silverpop_export_highest hg ON ex.email = hg.email
  -- this INNER JOIN limits us to only the highest values.
  INNER JOIN silverpop_email_map dedupe_table ON ex.id = dedupe_table.master_email_id
  -- there is a question whether we should just join in the live address table at this
  -- point or store the whole address. It seems query time is not much affected but less tables
  -- may be locked.
  LEFT JOIN silverpop_export_staging addr ON dedupe_table.address_id = addr.address_id
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
    ex.state = addr.state,
    -- get the one associated with the master email, failing that 'any'
    ex.preferred_language = COALESCE(ex.preferred_language, dedupe_table.preferred_language),
    -- this gets the 'max' - ie if ANY are 1 then we get that.
    ex.opted_out = dedupe_table.opted_out ;

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

-- Move the data from the staging table into the persistent one
-- Query OK, 19081073 rows affected (10 min 45.45 sec)
INSERT INTO silverpop_export (
  id,contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in, employer_id, employer_name,
  has_recurred_donation,highest_usd_amount,highest_native_amount,
  highest_native_currency,highest_donation_date,lifetime_usd_total,donation_count,
  latest_currency,latest_currency_symbol,latest_native_amount,
  latest_donation, first_donation_date,city,country,state,postal_code,
  total_2014, total_2015, total_2016, total_2017,
  total_2018, total_2019, total_2020, endowment_last_donation_date, endowment_first_donation_date, endowment_number_donations)
SELECT id,contact_id,contact_hash,first_name,last_name,ex.preferred_language,ex.email,opted_in, employer_id, employer_name,
  has_recurred_donation,highest_usd_amount,highest_native_amount,
  highest_native_currency,highest_donation_date,lifetime_usd_total,donation_count,
  latest_currency,latest_currency_symbol,latest_native_amount,
  latest_donation,first_donation_date,city,country,state,postal_code,
  total_2014, total_2015, total_2016, total_2017,
  total_2018, total_2019, total_2020, endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations
FROM silverpop_export_staging ex
-- this inner join is restricting us to only one record per email.
-- currently it is the highest email_id. Ideally it will later to change to
-- email_id associated with the highest donation.
INNER JOIN silverpop_email_map dedupe_table ON ex.id = dedupe_table.master_email_id
WHERE ex.opted_out=0
AND (opted_in IS NULL OR opted_in = 1)

ON DUPLICATE KEY UPDATE email = silverpop_export.email;

-- Create a nice view to export from
CREATE OR REPLACE VIEW silverpop_export_view AS
  SELECT
    contact_id ContactID,
    e.contact_hash,
    email,
    IFNULL(e.first_name, '')                                               firstname,
    IFNULL(e.last_name, '')                                                lastname,
    CASE
      WHEN gender_id =1 THEN 'Female'
      WHEN gender_id =2 THEN 'Male'
      WHEN gender_id =3 THEN 'Transgender'
      ELSE ''
    END as gender,
    IFNULL(country, 'XX')                                                  country,
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
    IFNULL(DATE_FORMAT(IF (endowment_first_donation_date IS NULL OR first_donation_date < endowment_first_donation_date , first_donation_date, endowment_first_donation_date), '%m/%d/%Y'), '')
      as all_funds_first_donation_date,
    -- Placeholder, this requires extra work above to calculate.
    '' as all_funds_highest_donation_date,
    -- Placeholder, this requires extra work above to calculate.
    0 as all_funds_highest_usd_amount,
    IFNULL(DATE_FORMAT(IF (endowment_last_donation_date IS NULL OR highest_donation_date > endowment_last_donation_date , highest_donation_date, endowment_last_donation_date), '%m/%d/%Y'), '')
      as all_funds_latest_donation_date,
    0 as all_funds_latest_native_amount,
    IFNULL(DATE_FORMAT(endowment_last_donation_date, '%m/%d/%Y'), '') endowment_last_donation_date,
    IFNULL(DATE_FORMAT(endowment_first_donation_date, '%m/%d/%Y'), '') endowment_first_donation_date,
    endowment_number_donations,
    -- Placeholder, this requires extra work above to calculate.
    '' as endowment_highest_donation_date,
    -- Placeholder, this requires extra work above to calculate.
    0 as endowment_highest_native_amount,
    -- Placeholder, this requires extra work above to calculate.
    '' as endowment_highest_native_currency,
    -- Placeholder, this requires extra work above to calculate.
    0 as endowment_highest_usd_amount,
    -- Placeholder, this requires extra work above to calculate.
    '' as endowment_latest_currency,
    -- Placeholder, this requires extra work above to calculate.
    0 as endowment_latest_native_amount,

    donation_count as foundation_donation_count,
    IFNULL(DATE_FORMAT(first_donation_date, '%m/%d/%Y'), '') foundation_first_donation_date,
    IFNULL(DATE_FORMAT(highest_donation_date, '%m/%d/%Y'), '') foundation_highest_donation_date,
    highest_usd_amount as foundation_highest_usd_amount,
    IFNULL(DATE_FORMAT(latest_donation, '%m/%d/%Y'), '') foundation_latest_donation_date,
    latest_native_amount as foundation_latest_native_amount,
    highest_native_amount as foundation_highest_native_amount,
    highest_native_currency as foundation_highest_native_currency,
    lifetime_usd_total as foundation_lifetime_usd_total,
    latest_currency as foundation_latest_currency,
    latest_currency_symbol as foundation_latest_currency_symbol,
    IF(has_recurred_donation, 'YES', 'NO') as foundation_has_recurred_donation,
    total_2014 as foundation_total_2014,
    total_2015 as foundation_total_2015,
    total_2016 as foundation_total_2016,
    total_2017 as foundation_total_2017,
    total_2018 as foundation_total_2018,
    total_2019 as foundation_total_2019,
    total_2020 as foundation_total_2020

  FROM silverpop_export e
  LEFT JOIN civicrm.civicrm_value_1_prospect_5 v ON v.entity_id = contact_id
  LEFT JOIN civicrm.civicrm_contact c ON c.id = contact_id;
