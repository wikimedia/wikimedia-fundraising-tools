SET autocommit = 1;
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

-- The point of silverpop_export is presumably that it is more performant than skipping straight to silverpop_export_view
-- although I believe that theory needs testing.

-- Create a table of countries and languages for contacts with no country
-- pulling data from contribution tracking.
-- Query OK, 369156 rows affected (2 min 59.66 sec)
INSERT INTO silverpop_missing_countries
-- The use of MAX for country really means 'any', for lang it should help avoid NULL.
SELECT c.contact_id, MAX(ct.country), MAX(lang) FROM civicrm.civicrm_contribution c
  LEFT JOIN drupal.contribution_tracking ct ON c.id = ct.contribution_id
  LEFT JOIN silverpop_countrylangs langs ON langs.country = ct.country
  LEFT JOIN civicrm.civicrm_address a ON a.contact_id = c.contact_id AND a.is_primary = 1
WHERE ct.country IS NOT NULL
  AND a.country_id IS NULL
GROUP BY c.contact_id;

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
-- Query OK, 23986414 rows affected (23 min 34.44 sec)
INSERT INTO silverpop_export_staging
  (id, modified_date, contact_id, contact_hash, email, first_name, last_name, preferred_language, opted_out, opted_in,
   employer_id, employer_name, address_id, city, postal_code, country, state, all_funds_latest_donation_date)
  SELECT
    e.id,
    c.modified_date,
    e.contact_id, c.hash, e.email, c.first_name, c.last_name,
    REPLACE(COALESCE(c.preferred_language, cl.lang, 'en'), '_', '-') as preferred_language,
    (c.is_opt_out OR c.do_not_email OR e.on_hold OR COALESCE(v.do_not_solicit, 0)) as opted_out,
    v.opt_in as opted_in,
    c.employer_id,
    IF(c.employer_id, c.organization_name, '') as employer_name,
    a.id as address_id,
    a.city,
    a.postal_code,
    COALESCE(ctry.iso_code, s.country) as country,
    st.name as state,
    IF((donor.endowment_last_donation_date IS NULL OR donor.last_donation_date > donor.endowment_last_donation_date), donor.last_donation_date, donor.endowment_last_donation_date) as all_funds_latest_donation_date
  FROM civicrm.civicrm_email e
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id
  LEFT JOIN civicrm.civicrm_value_1_communication_4 v ON v.entity_id = c.id
  LEFT JOIN civicrm.civicrm_address a ON a.contact_id = e.contact_id AND a.is_primary = 1
  LEFT JOIN silverpop_missing_countries s ON s.contact_id = e.contact_id
  LEFT JOIN civicrm.civicrm_country ctry
            ON a.country_id = ctry.id
  LEFT JOIN civicrm.civicrm_state_province st
            ON a.state_province_id = st.id
  LEFT JOIN silverpop_countrylangs cl ON cl.country_unicode = ctry.iso_code
  LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = e.contact_id
  WHERE
    e.email IS NOT NULL AND e.email != ''
    AND c.is_deleted = 0
    AND e.is_primary = 1
;

-- Query OK, 23199001 rows affected (11 min 55.19 sec)
INSERT INTO silverpop_email_map
  SELECT email,
    MAX(id) as master_email_id,
    MAX(address_id) as address_id,
    MAX(preferred_language) as preferred_language,
    MAX(opted_out) as opted_out,
    # 0 if they have ever actually opted out, else 1
    # we use this for filtering so don't need to preserve the nuance.
    # This should be revisited per https://phabricator.wikimedia.org/T256522
    MIN(IF (opted_in = 0, 0, 1)) as opted_in
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
-- Query OK, 19160114 rows affected, 11 warnings (10 min 15.50 sec)
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
-- @todo - speed test without the second desc.
  ORDER BY last_donation_date DESC, d.last_donation_usd DESC
ON DUPLICATE KEY UPDATE latest_currency = silverpop_export_latest.latest_currency;

-- Populate table for highest donation amount and date
-- Query OK, 19160133 rows affected, 78 warnings (26 min 24.83 sec)
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
-- Query OK, 23198921 rows affected (42 min 32.54 sec)
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
  FROM civicrm.civicrm_email e FORCE INDEX(UI_email)
  LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = e.contact_id
  # We need to be careful with this group by. We want the sum by email but we don't want
  # any other left joins that could be 1 to many & inflate the aggregates.
  GROUP BY e.email;

-- Query OK, 869024 rows affected (56.89 sec)
INSERT INTO silverpop_endowment_latest
SELECT
  email.email,
  -- really we want the currency/amount associated with the highest amount on
  -- the highest date but the chances of 2 concurrent donations
  -- with different currencies are neglible
  -- so the value of handling currency better here is low.
  MAX(extra.original_currency) as endowment_latest_currency,
  MAX(extra.original_amount) as endowment_latest_native_amount
FROM  silverpop_export_stat export
        LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
        LEFT JOIN civicrm.civicrm_contribution c ON  c.contact_id = email.contact_id
        LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
WHERE c.receive_date = export.endowment_last_donation_date
  AND export.endowment_last_donation_date IS NOT NULL
  AND c.financial_type_id = 26
  AND c.contribution_status_id = 1
  AND c.total_amount > 0
GROUP BY email.email;

-- Query OK, 869058 rows affected (2 min 52.56 sec)
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
FROM  silverpop_export_stat export
  LEFT JOIN civicrm.civicrm_email email ON email.email = export.email AND email.is_primary = 1
  LEFT JOIN civicrm.civicrm_contribution c FORCE INDEX(received_date) ON  c.contact_id = email.contact_id
  LEFT JOIN civicrm.wmf_contribution_extra extra ON extra.entity_id = c.id
WHERE c.total_amount = export.endowment_highest_usd_amount
  AND export.endowment_highest_usd_amount > 0
  AND c.financial_type_id = 26
  AND c.contribution_status_id = 1
GROUP BY email.email;

-- Query OK, 492395 rows affected (24.78 sec)
INSERT INTO silverpop_has_recur
 SELECT DISTINCT email, 1 as has_recurred_donation FROM
   civicrm.civicrm_contribution_recur recur
 INNER JOIN civicrm.civicrm_contribution contributions
   ON recur.id = contributions.contribution_recur_id
   AND contributions.contribution_status_id = 1
   AND contributions.financial_type_id != 26
   AND contributions.total_amount > 0
 INNER JOIN civicrm.civicrm_email email ON recur.contact_id = email.contact_id AND is_primary = 1;

-- Do an extra delete in case there was a timing issue
-- in deployment we have seen cases where a contact is part way through a manual merge.
-- the to-be-primary has been saved but the is_primary is not yet removed from the prior primary
-- I think https://gerrit.wikimedia.org/r/c/wikimedia/fundraising/tools/+/609238 should
-- make this obsolete. But, for ow....
SET @offSetInDays = 1;
DELETE s
FROM silverpop_export_staging s
       LEFT JOIN civicrm.log_civicrm_email l
                 ON s.id = l.id
       LEFT JOIN civicrm.civicrm_email e
  -- use is_primary in case they are no longer primary
                 ON s.id = e.id  AND e.is_primary = 1
WHERE l.log_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
  AND e.email IS NULL OR e.email = '';

-- Move the data from the staging table into the persistent one
-- Query OK, 19044058 rows affected, 152 warnings (26 min 37.42 sec)
INSERT INTO silverpop_export (
  id,contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in, employer_id, employer_name,
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
SELECT ex.id,ex.contact_id,ex.contact_hash,ex.first_name,ex.last_name,
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
FROM silverpop_export_staging ex
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
AND (ex.opted_in IS NULL OR ex.opted_in = 1);

-- Query OK, 0 rows affected (0.00 sec)
-- Create a nice view to export from
CREATE OR REPLACE VIEW silverpop_export_view AS
  SELECT
    contact_id ContactID,
    e.contact_hash,
    e.email,
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
    foundation_total_2020 as foundation_total_2020

  FROM silverpop_export e
  LEFT JOIN civicrm.civicrm_value_1_prospect_5 v ON v.entity_id = contact_id
  LEFT JOIN civicrm.civicrm_contact c ON c.id = contact_id
  LEFT JOIN silverpop_endowment_latest endow_late ON endow_late.email = e.email
  LEFT JOIN silverpop_endowment_highest endow_high ON endow_high.email = e.email
;
