-- If we need to rebuild the staging table we can use this.
-- It is not currently called from anywhere as we are using incremental updates
-- but has been retained in a separate file in case we need to rebuild the whole table.

-- Create a table of countries and languages for contacts with no country
-- pulling data from contribution tracking.
-- Query OK, 369156 rows affected (2 min 59.66 sec)
-- Create a table of countries and languages for contacts with no country
-- pulling data from contribution tracking.
-- Query OK, 369156 rows affected (2 min 59.66 sec)
INSERT INTO silverpop_missing_countries
-- The use of MAX for country really means 'any', for lang it should help avoid NULL.
SELECT c.contact_id, MAX(ct.country), MAX(lang) FROM civicrm.civicrm_contribution c
                                                       LEFT JOIN civicrm.civicrm_contribution_tracking ct ON c.id = ct.contribution_id
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
  COALESCE(ctry.iso_code, mc.country) as country,
  st.name as state,
  IF((donor.endowment_last_donation_date IS NULL OR donor.last_donation_date > donor.endowment_last_donation_date), donor.last_donation_date, donor.endowment_last_donation_date) as all_funds_latest_donation_date
FROM civicrm.civicrm_email e
--  LEFT JOIN silverpop_export_staging staging ON staging.id = e.id
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id
  LEFT JOIN civicrm.civicrm_value_1_communication_4 v ON v.entity_id = c.id
  LEFT JOIN civicrm.civicrm_address a ON a.contact_id = e.contact_id AND a.is_primary = 1
  LEFT JOIN silverpop_missing_countries mc ON mc.contact_id = e.contact_id
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
  #AND c.modified_date BETWEEN @startDate AND @endDate
  #AND staging.id IS NULL
;
