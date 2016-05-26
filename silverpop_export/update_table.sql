-- Updates the silverpop_export table
--
-- TODO: Most of the complexity will go away once our contacts' exact email
-- matches have been deduped.
--
-- Timing is from a 2016-04-07 production job.

SET autocommit = 1;

DROP TABLE IF EXISTS silverpop_export_staging;
DROP TABLE IF EXISTS silverpop_export_latest;
DROP TABLE IF EXISTS silverpop_export_dedupe_email;
DROP TABLE IF EXISTS silverpop_export_stat;

CREATE TABLE IF NOT EXISTS silverpop_export_staging(
  id int unsigned PRIMARY KEY,  -- This is actually civicrm_email.id

  -- General information about the contact
  contact_id int unsigned,
  first_name varchar(128),
  last_name varchar(128),
  preferred_language varchar(5),
  email varchar(255),
  opted_out tinyint(1),

  -- Lifetime contribution statistics
  has_recurred_donation tinyint(1),
  highest_usd_amount decimal(20,2),
  lifetime_usd_total decimal(20,2),
  donation_count int,

  -- Latest contribution statistics
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,

  -- Address information
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128),

  INDEX spex_contact_id (contact_id),
  INDEX spex_email (email),
  INDEX spex_city (city),
  INDEX spex_country (country),
  INDEX spex_opted_out (opted_out)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_export_latest(
  email varchar(255) PRIMARY KEY,
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime
) COLLATE 'utf8_unicode_ci';

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
-- (15 minutes)
INSERT INTO silverpop_export_staging
  (id, contact_id, email, first_name, last_name, preferred_language, opted_out)
  SELECT
    e.id, e.contact_id, e.email, c.first_name, c.last_name,
    IF(SUBSTRING(c.preferred_language, 1, 1) = '_', 'en', SUBSTRING(c.preferred_language, 1, 2)),
    (c.is_opt_out OR c.do_not_email OR e.on_hold OR COALESCE(d.do_not_solicit, 0))
  FROM civicrm.civicrm_email e
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id
  LEFT JOIN civicrm.wmf_donor d ON d.entity_id = c.id
  WHERE
    e.email IS NOT NULL AND e.email != ''
    AND c.is_deleted = 0
    AND e.is_primary = 1;

-- Find the latest donation for each email address. Ordering by
-- recieve_date and total_amount descending should always insert 
-- the latest donation first, with the larger prevailing for an
-- email with multiple simultaneous donations. All the rest for
-- that email will be ignored due to the unique constraint. We
-- use 'ON DUPLICATE KEY UPDATE' instead of 'INSERT IGNORE' as
-- the latter throws warnings.
-- (12 minutes)
INSERT INTO silverpop_export_latest
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
    ct.contribution_status_id = 1 -- 'Completed'
  ORDER BY
    ct.receive_date DESC,
    ct.total_amount DESC
ON DUPLICATE KEY UPDATE latest_currency = silverpop_export_latest.latest_currency;

-- Populate data from contribution tracking, because that's fairly
-- reliable. Do this before deduplication so we can attempt to make
-- intelligent fallbacks in case of null data
-- (11 minutes)
UPDATE
    silverpop_export_staging ex,
    civicrm.civicrm_contribution ct,
    drupal.contribution_tracking dct
  SET
    ex.preferred_language = dct.language
  WHERE
    ex.contact_id = ct.contact_id AND
    dct.contribution_id = ct.id AND
    dct.language IS NOT NULL;

-- (15 minutes)
UPDATE
    silverpop_export_staging ex,
    civicrm.civicrm_contribution ct,
    drupal.contribution_tracking dct
  SET
    ex.country = dct.country
  WHERE
    ex.contact_id = ct.contact_id AND
    dct.contribution_id = ct.id AND
    dct.country IS NOT NULL;

-- Deduplicate rows that have the same email address, we will
-- have to merge in more data later, but this is ~1.5M rows we're
-- getting rid of here which is more better than taking them all the way
-- through.
CREATE TABLE silverpop_export_dedupe_email (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email varchar(255),
  maxid int,
  preferred_language varchar(5),
  country varchar(2),
  opted_out tinyint(1),

  INDEX spexde_email (email)
) COLLATE 'utf8_unicode_ci';

INSERT INTO silverpop_export_dedupe_email (email, maxid, opted_out)
   SELECT email, max(id) maxid, max(opted_out) opted_out
     FROM silverpop_export_staging
       FORCE INDEX (spex_email)
       GROUP BY email
       HAVING count(*) > 1;

-- We pull in language/country from the parent table so that we
-- can preserve them and not propogate nulls
UPDATE silverpop_export_dedupe_email exde, silverpop_export_staging ex
  SET
    exde.preferred_language = ex.preferred_language
  WHERE
    ex.email = exde.email AND
    ex.preferred_language IS NOT NULL;

UPDATE silverpop_export_dedupe_email exde, silverpop_export_staging ex
  SET
    exde.country = ex.country
  WHERE
    ex.email = exde.email AND
    ex.country IS NOT NULL;

DELETE silverpop_export_staging FROM silverpop_export_staging, silverpop_export_dedupe_email
  WHERE
    silverpop_export_staging.email = silverpop_export_dedupe_email.email AND
    silverpop_export_staging.id != silverpop_export_dedupe_email.maxid;

UPDATE silverpop_export_staging ex, silverpop_export_dedupe_email exde
  SET
    ex.opted_out = exde.opted_out,
    ex.preferred_language = exde.preferred_language,
    ex.country = exde.country
  WHERE
    exde.maxid = ex.id;

-- Create an aggregate table from a full contribution table scan
CREATE TABLE silverpop_export_stat (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email varchar(255),
  exid INT,                         -- STEP 5
  max_amount_usd decimal(20,2),     -- STEP 5
  has_recurred_donation tinyint(1),
  total_usd decimal(20,2),          -- STEP 5
  cnt_total int unsigned,

  INDEX spexs_email (email)
) COLLATE 'utf8_unicode_ci';

-- (30 minutes)
INSERT INTO silverpop_export_stat
  (email, exid, max_amount_usd, total_usd, cnt_total, has_recurred_donation)
  SELECT
    e.email, MAX(ex.id), MAX(ct.total_amount), SUM(ct.total_amount),
    count(*),
    MAX(IF(SUBSTRING(ct.trxn_id, 1, 9) = 'RECURRING', 1, 0))
  FROM civicrm.civicrm_email e FORCE INDEX(UI_email)
  JOIN silverpop_export_staging ex ON e.email=ex.email
  JOIN civicrm.civicrm_contribution ct ON e.contact_id=ct.contact_id
  WHERE ct.total_amount IS NOT NULL
  GROUP BY e.email;

-- (10 minutes)
UPDATE silverpop_export_staging ex, silverpop_export_stat exs
  SET
    ex.highest_usd_amount = exs.max_amount_usd,
    ex.lifetime_usd_total = exs.total_usd,
    ex.donation_count = exs.cnt_total,
    ex.has_recurred_donation = IF(exs.has_recurred_donation > 0, 1, 0)
  WHERE
    ex.id = exs.exid;

-- Populate information about the most recent contribution
UPDATE silverpop_export_staging ex, silverpop_export_latest ct
  SET
    ex.latest_currency = ct.latest_currency,
    ex.latest_native_amount = ct.latest_native_amount,
    ex.latest_usd_amount = ct.latest_usd_amount,
    ex.latest_donation = ct.latest_donation
  WHERE
    ex.email = ct.email;

-- Remove contacts who apparently have no contributions
-- Leave opted out non-contributors so we don't spam anyone
DELETE FROM silverpop_export_staging
  WHERE
    silverpop_export_staging.latest_donation IS NULL AND
    silverpop_export_staging.opted_out = 0;

-- Join on civicrm address where we do not already have a geolocated
-- address from contribution tracking
UPDATE silverpop_export_staging ex
  JOIN civicrm.civicrm_address addr ON ex.contact_id = addr.contact_id
  JOIN civicrm.civicrm_country ctry ON addr.country_id = ctry.id
  LEFT JOIN civicrm.civicrm_state_province st ON addr.state_province_id = st.id
  SET
    ex.city = addr.city,
    ex.country = ctry.iso_code,
    ex.postal_code = addr.postal_code,
    ex.state = st.name
  WHERE
    ex.country IS NULL AND
    ex.opted_out = 0;

-- And now updated by civicrm address where we have a country but no
-- city from contribution tracking.  The countries must match.
-- (11 minutes)
UPDATE silverpop_export_staging ex
  JOIN civicrm.civicrm_address addr ON ex.contact_id = addr.contact_id
  JOIN civicrm.civicrm_country ctry
       ON addr.country_id = ctry.id
       AND ex.country = ctry.iso_code
  LEFT JOIN civicrm.civicrm_state_province st ON addr.state_province_id = st.id
  SET
    ex.city = addr.city,
    ex.postal_code = addr.postal_code,
    ex.state = st.name
  WHERE
    ex.city IS NULL AND
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

-- Normalize the data prior to final export
UPDATE silverpop_export_staging SET preferred_language='en' WHERE preferred_language IS NULL;
UPDATE silverpop_export_staging SET
    highest_usd_amount = 0,
    lifetime_usd_total = 0,
    donation_count = 0,
    latest_currency = 'USD',
    latest_native_amount = 0,
    latest_usd_amount = 0,
    latest_donation = NOW(),
    has_recurred_donation = 0
  WHERE donation_count IS NULL AND opted_out = 0;
UPDATE silverpop_export_staging SET country='US' where country IS NULL AND opted_out = 0;

--
-- Collect email addresses which should be excluded for various reasons, such as:
-- * Exclude non-primary addresses
-- * Exclude any "former residence" email addresses.
-- * Exclude addresses dropped during contact merge.
--
DROP TABLE IF EXISTS silverpop_excluded;

CREATE TABLE IF NOT EXISTS silverpop_excluded(
  id int AUTO_INCREMENT PRIMARY KEY,
  email varchar(255),

  INDEX sx_email (email),
  CONSTRAINT sx_email_u UNIQUE (email)
) COLLATE 'utf8_unicode_ci' AUTO_INCREMENT=1;

-- Same no-op update trick as with silverpop_export_latest
INSERT INTO silverpop_excluded (email)
  SELECT email
    FROM log_civicrm.log_civicrm_email e
ON DUPLICATE KEY UPDATE email = silverpop_excluded.email;

DELETE silverpop_excluded
  FROM silverpop_excluded
  JOIN silverpop_export_staging s
    ON s.email = silverpop_excluded.email
    WHERE s.opted_out = 0;

-- We don't want to suppress emails of Civi users.
-- Conveniently, the account name is the email address in
-- in the table that associates contacts with accounts.
DELETE silverpop_excluded
  FROM silverpop_excluded
  JOIN civicrm.civicrm_uf_match m
    ON m.uf_name = silverpop_excluded.email;

-- Prepare the persistent export table.
DROP TABLE IF EXISTS silverpop_export;

CREATE TABLE IF NOT EXISTS silverpop_export(
  id int unsigned PRIMARY KEY,  -- This is actually civicrm_email.id

  -- General information about the contact
  contact_id int unsigned,
  first_name varchar(128),
  last_name varchar(128),
  preferred_language varchar(5),
  email varchar(255),

  -- Lifetime contribution statistics
  has_recurred_donation tinyint(1),
  highest_usd_amount decimal(20,2),
  lifetime_usd_total decimal(20,2),
  donation_count int,

  -- Latest contribution statistics
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,

  -- Address information
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128),

  INDEX rspex_contact_id (contact_id),
  INDEX rspex_email (email),
  INDEX rspex_city (city),
  INDEX rspex_country (country),
  INDEX rspex_postal (postal_code),
  CONSTRAINT sp_email UNIQUE (email)
) COLLATE 'utf8_unicode_ci';

-- Move the data from the staging table into the persistent one
-- (12 minutes)
INSERT INTO silverpop_export (
  id,contact_id,first_name,last_name,preferred_language,email,
  has_recurred_donation,highest_usd_amount,lifetime_usd_total,donation_count,
  latest_currency,latest_native_amount,latest_usd_amount,latest_donation,
  city,country,state,postal_code )
SELECT id,contact_id,first_name,last_name,preferred_language,email,
  has_recurred_donation,highest_usd_amount,lifetime_usd_total,donation_count,
  latest_currency,latest_native_amount,latest_usd_amount,latest_donation,
  city,country,state,postal_code
FROM silverpop_export_staging
WHERE opted_out=0;

-- Create a nice view to export from
CREATE OR REPLACE VIEW silverpop_export_view AS
  SELECT
    contact_id ContactID,
    email,
    IFNULL(first_name, '') firstname,
    IFNULL(last_name, '') lastname,
    country,
    state,
    postal_code,
    SUBSTRING(preferred_language, 1, 2) IsoLang,
    IF(has_recurred_donation, 'YES', 'NO') has_recurred_donation,
    highest_usd_amount,
    lifetime_usd_total,
    DATE_FORMAT(latest_donation, '%m/%d/%Y') latest_donation_date,
    latest_usd_amount,
    latest_currency,
    latest_native_amount,
    donation_count
  FROM silverpop_export;
