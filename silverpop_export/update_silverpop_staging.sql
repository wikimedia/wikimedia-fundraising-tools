-- This file manages the incremental updates to the main contact list in the silverpop_staging table.
-- It only takes a few minutes run at on off-peak time of year. It's not known how long
-- it will take when more updates are happening. Most of the time take is in the
-- last statement which removes any emails not found in civicrm_email.
-- The purpose of this is to remove any emails no longer in the database, which would
-- be the case for a privacy delete. Since we are maintaining the email.id field
-- we should be able to rely on that for this purpose.

-- The intention is that this would be run more often than once a day and need not
-- be only called from 'the whole script'

-- Default offset - we can maybe pass this in from python.
-- I think 2 (2 days) is probably the right value but a higher value for now
-- reduces the risk a script fails and we don't notice.
SET @offSetInDays = 7;


-- Drop and recreate the table tracking updated emails.
-- if the rebuild fails for some reason this table will be empty
-- an attempts to incrementally build other tables (like stats) will also fail.
-- this is a good thing - we want to be able to rebuild more granularly but with internal integrity.
DROP TABLE IF EXISTS silverpop_update_world;
CREATE TABLE silverpop_update_world (
  `email` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `email` (`email`)
) ENGINE=InnoDB;
-- We use a transaction to keep this table consistently updated to the same point in time.
-- The email suppression list uses MAX(id) from this table as it's upper bound so we want
-- the table to be in a consistent state up to that point in time. If something fails
-- the whole commit fails.

-- Create a table of countries and languages for contacts with no country
-- pulling data from contribution tracking.
-- low volume of 'new catches'
-- Query OK, 35 rows affected (6.29 sec)
INSERT INTO silverpop_missing_countries
-- The use of MAX for country really means 'any', for lang it should help avoid NULL.
SELECT c.contact_id, MAX(ct.country), MAX(lang)
FROM civicrm.civicrm_contribution c
   LEFT JOIN drupal.contribution_tracking ct ON c.id = ct.contribution_id
   LEFT JOIN silverpop_countrylangs langs ON langs.country = ct.country
   LEFT JOIN civicrm.civicrm_address a ON a.contact_id = c.contact_id AND a.is_primary = 1
   LEFT JOIN civicrm.civicrm_contact contact ON contact.id = c.contact_id
WHERE ct.country IS NOT NULL
  AND a.country_id IS NULL
  AND contact.modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
GROUP BY c.contact_id;

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
-- Query OK, 30 rows affected (1.16 sec)
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
   LEFT JOIN silverpop_export_staging staging ON staging.id = e.id
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
  AND c.modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
  AND staging.id IS NULL
;

-- Update any contacts in the staging table
-- Query OK, 23123 rows affected (2.08 sec)
UPDATE silverpop_export_staging s
  INNER JOIN civicrm.civicrm_email e  ON s.id = e.id
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id AND e.is_primary = 1
  LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = e.contact_id
  LEFT JOIN civicrm.civicrm_value_1_communication_4 v ON v.entity_id = c.id
  LEFT JOIN civicrm.civicrm_address a ON a.contact_id = e.contact_id AND a.is_primary = 1
  LEFT JOIN silverpop_missing_countries mc ON mc.contact_id = e.contact_id
  LEFT JOIN civicrm.civicrm_country ctry
    ON a.country_id = ctry.id
  LEFT JOIN civicrm.civicrm_state_province st
    ON a.state_province_id = st.id
  LEFT JOIN silverpop_countrylangs cl ON cl.country_unicode = ctry.iso_code
SET
    s.id = e.id,
    s.modified_date = c.modified_date,
    s.contact_id = e.contact_id,
    s.contact_hash = c.hash,
    s.email = e.email,
    s.first_name = c.first_name,
    s.last_name = c.last_name,
    s.preferred_language = REPLACE(COALESCE(c.preferred_language, cl.lang, 'en'), '_', '-') ,
    s.opted_out = (c.is_opt_out OR c.do_not_email OR e.on_hold OR COALESCE(v.do_not_solicit, 0)),
    s.opted_in = v.opt_in,
    s.employer_id = c.employer_id,
    s.employer_name = IF(c.employer_id, c.organization_name, ''),
    s.address_id = a.id,
    s.city = a.city,
    s.postal_code = a.postal_code,
    s.country = COALESCE(ctry.iso_code, mc.country),
    s.state = st.name,
    s.all_funds_latest_donation_date = IF((donor.endowment_last_donation_date IS NULL OR donor.last_donation_date > donor.endowment_last_donation_date), donor.last_donation_date, donor.endowment_last_donation_date)
WHERE
  e.email IS NOT NULL AND e.email != ''
  AND c.is_deleted = 0
  AND c.modified_date > DATE_SUB(NOW(), INTERVAL  @offSetInDays DAY)
  AND e.is_primary = 1;

-- Get rid of any emails actually deleted since interval
-- There might not be a record in the civicrm_contact table so we
-- use the log_civicrm_email to narrow down the rows we are looking at.
-- Query OK, 2955 rows affected (0.32 sec)
DELETE s
FROM silverpop_export_staging s
  LEFT JOIN civicrm.log_civicrm_email l
    ON s.id = l.id
  LEFT JOIN civicrm.civicrm_email e
    -- use is_primary in case they are no longer primary
    ON s.id = e.id  AND e.is_primary = 1
WHERE l.log_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
  AND e.email IS NULL OR e.email = '';

-- Delete any emails associated with contacts that have been deleted.....
-- if the email has been deleted it should be picked up above and
-- if it was moved to a different contact that is not deleted
-- the update should have picked that up. However, if the contact
-- it is associated with is deleted we need to remove it.
-- Query OK, 1028 rows affected (0.35 sec)
DELETE s
FROM silverpop_export_staging s
  LEFT JOIN civicrm.civicrm_email e
    ON s.id = e.id
  LEFT JOIN civicrm.civicrm_contact c ON c.id = e.contact_id
WHERE c.modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
  AND c.is_deleted = 1;

-- Delete any emails that have been recorded in the deleted emails table
-- Query OK, 3 rows affected (1 min 31.44 sec)
DELETE FROM silverpop_export_staging
WHERE id IN (SELECT id from civicrm.civicrm_deleted_email);

COMMIT;

-- Create list of emails to update
-- runs fast when not many to do - ie on staging with 7 days interval (it's must faster with just 2)
-- Query OK, 804581 rows affected (33.15 sec)
INSERT INTO silverpop_update_world SELECT DISTINCT email
FROM silverpop_export_staging
WHERE modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY);


