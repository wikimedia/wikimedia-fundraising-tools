-- Updates the silverpop_export table
--
-- TODO: Most of the complexity will go away once our contacts' exact email
-- matches have been deduped.
--
-- Timing is from a 2019-08-22 Staging test.

SET autocommit = 1;

DROP TABLE IF EXISTS silverpop_excluded;
DROP TABLE IF EXISTS silverpop_export_staging;
DROP TABLE IF EXISTS silverpop_export_latest;
DROP TABLE IF EXISTS silverpop_export_highest;
DROP TABLE IF EXISTS silverpop_export_dedupe_email;
DROP TABLE IF EXISTS silverpop_export_stat;
DROP TABLE IF EXISTS silverpop_export_address;

CREATE TABLE IF NOT EXISTS silverpop_export_staging(
  id int unsigned PRIMARY KEY,  -- This is actually civicrm_email.id

  -- General information about the contact
  contact_id int unsigned,
  contact_hash varchar(32),
  first_name varchar(128),
  last_name varchar(128),
  preferred_language varchar(12),
  email varchar(255),
  opted_out tinyint(1),
  opted_in tinyint(1),

  -- Lifetime contribution statistics
  has_recurred_donation tinyint(1) not null default 0,
  highest_usd_amount decimal(20,2) not null default 0,
  highest_native_amount decimal(20,2) not null default 0,
  highest_native_currency varchar(3) not null default '',
  lifetime_usd_total decimal(20,2) not null default 0,
  donation_count int not null default 0,

  -- Aggregate contribution statistics
  -- Sadly these would need updating next year. I have doubts about doing something more
  -- clever without reviewing the script more broadly as it's kinda tricky in straight sql
  total_2014 decimal(20,2) not null default 0,
  total_2015 decimal(20,2) not null default 0,
  total_2016 decimal(20,2) not null default 0,
  total_2017 decimal(20,2) not null default 0,
  total_2018 decimal(20,2) not null default 0,
  total_2019 decimal(20,2) not null default 0,
  total_2020 decimal(20,2) not null default 0,

  -- Endowment stats ----
  endowment_last_donation_date datetime null,
  endowment_first_donation_date datetime null,
  endowment_number_donations  decimal(20,2) not null default 0,

  -- Latest contribution statistics
  latest_currency varchar(3) not null default '',
  latest_currency_symbol varchar(8) not null default '',
  latest_native_amount decimal(20,2) not null default 0,
  latest_usd_amount decimal(20,2) not null default 0,
  latest_donation datetime null,
  first_donation_date datetime null,
  highest_donation_date datetime null,

  -- Address information
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128),
  timezone varchar(8),

  INDEX spex_contact_id (contact_id),
  INDEX spex_email (email),
  INDEX spex_country (country),
  INDEX spex_opted_out (opted_out)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_export_latest(
  email varchar(255) PRIMARY KEY,
  latest_currency varchar(3),
  latest_currency_symbol varchar(8),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime
) COLLATE 'utf8_unicode_ci';

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
-- (11 min 17.25 sec)
INSERT INTO silverpop_export_staging
  (id, contact_id, contact_hash, email, first_name, last_name, preferred_language, opted_out, opted_in)
  SELECT
    e.id, e.contact_id, c.hash, e.email, c.first_name, c.last_name,
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

CREATE TABLE IF NOT EXISTS silverpop_excluded(
  id int AUTO_INCREMENT PRIMARY KEY,
  email varchar(255),

  INDEX sx_email (email),
  CONSTRAINT sx_email_u UNIQUE (email)
) COLLATE 'utf8_unicode_ci' AUTO_INCREMENT=1;

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
    d.last_donation_usd,
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

CREATE TABLE silverpop_export_highest(
  email varchar(255) PRIMARY KEY,
  highest_native_currency varchar(3),
  highest_native_amount decimal(20,2),
  highest_usd_amount decimal(20,2),
  highest_donation_date datetime
) COLLATE 'utf8_unicode_ci';

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
    ct.contribution_status_id = 1 -- 'Completed'
  ORDER BY
    ct.total_amount DESC,
    ct.receive_date DESC
ON DUPLICATE KEY UPDATE highest_native_currency = silverpop_export_highest.highest_native_currency;

-- Deduplicate rows that have the same email address, we will
-- have to merge in more data later, but this is ~1.5M rows we're
-- getting rid of here which is more better than taking them all the way
-- through.
CREATE TABLE silverpop_export_dedupe_email (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email varchar(255),
  maxid int,
  preferred_language varchar(12),
  opted_out tinyint(1),

  INDEX spexde_email (email)
) COLLATE 'utf8_unicode_ci';

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

-- (1 min 2.15 sec
DELETE silverpop_export_staging FROM silverpop_export_staging, silverpop_export_dedupe_email
  WHERE
    silverpop_export_staging.email = silverpop_export_dedupe_email.email AND
    silverpop_export_staging.id != silverpop_export_dedupe_email.maxid;

--  (18.34 sec)
UPDATE silverpop_export_staging ex, silverpop_export_dedupe_email exde
  SET
    ex.opted_out = exde.opted_out,
    ex.preferred_language = exde.preferred_language
  WHERE
    exde.maxid = ex.id;

-- Create an aggregate table from a full contribution table scan
CREATE TABLE silverpop_export_stat (
  email varchar(255) PRIMARY KEY,
  exid INT,
  has_recurred_donation tinyint(1),
  total_usd decimal(20,2),
  cnt_total int unsigned,
  first_donation_date datetime,
    -- Aggregate contribution statistics
  total_2014 decimal(20,2) not null default 0,
  total_2015 decimal(20,2) not null default 0,
  total_2016 decimal(20,2) not null default 0,
  total_2017 decimal(20,2) not null default 0,
  total_2018 decimal(20,2) not null default 0,
  total_2019 decimal(20,2) not null default 0,
  total_2020 decimal(20,2) not null default 0,
  endowment_last_donation_date datetime null,
  endowment_first_donation_date datetime null,
  endowment_number_donations  decimal(20,2) not null default 0,
  INDEX stat_exid (exid)
) COLLATE 'utf8_unicode_ci';

-- 44 min 41.82 sec
INSERT INTO silverpop_export_stat
  (email, exid, total_usd, cnt_total, has_recurred_donation, first_donation_date,
   total_2014, total_2015, total_2016, total_2017,
   total_2018, total_2019, total_2020,
   endowment_last_donation_date, endowment_first_donation_date, endowment_number_donations
  )
  SELECT
    e.email,
    MAX(ex.id),
    SUM(donor.lifetime_usd_total) as lifetime_usd_total,
    SUM(donor.number_donations) as number_donations,
    MAX(IF(SUBSTRING(ct.trxn_id, 1, 9) = 'RECURRING', 1, 0)),
    MIN(donor.first_donation_date) as first_donation_date,
    SUM(donor.total_2014) as total_2014,
    SUM(donor.total_2015) as total_2015,
    SUM(donor.total_2016) as total_2016,
    SUM(donor.total_2017) as total_2017,
    SUM(donor.total_2018) as total_2018,
    SUM(donor.total_2019) as total_2019,
    SUM(donor.total_2020) as total_2020,
    MAX(donor.endowment_last_donation_date) as endowment_last_donation_date,
    MIN(donor.endowment_first_donation_date) as endowment_first_donation_date,
    SUM(donor.endowment_number_donations) as endowment_number_donations
  FROM civicrm.civicrm_email e FORCE INDEX(UI_email)
  JOIN silverpop_export_staging ex ON e.email=ex.email
  JOIN civicrm.civicrm_contribution ct ON e.contact_id=ct.contact_id
  LEFT JOIN civicrm.wmf_donor donor ON donor.entity_id = ct.contact_id
  WHERE ct.receive_date IS NOT NULL AND
    ct.total_amount > 0 AND -- Refunds don't count
    ct.contribution_status_id = 1 -- Only completed status
  GROUP BY e.email;

-- Postal addresses by email
CREATE TABLE silverpop_export_address (
  email varchar(255) PRIMARY KEY,
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128),
  timezone varchar(8)
) COLLATE 'utf8_unicode_ci';

-- (16 minutes)
-- Get latest address for each email.
INSERT INTO silverpop_export_address
SELECT      e.email, a.city, ctry.iso_code, st.name, a.postal_code, a.timezone
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
    ex.latest_usd_amount = COALESCE(lt.latest_usd_amount, 0),
    ex.latest_donation = lt.latest_donation,
    ex.highest_native_currency = COALESCE(hg.highest_native_currency, ''),
    ex.highest_native_amount = COALESCE(hg.highest_native_amount, 0),
    ex.highest_usd_amount = COALESCE(hg.highest_usd_amount, 0),
    ex.highest_donation_date = hg.highest_donation_date,
    ex.city = addr.city,
    ex.country = addr.country,
    ex.postal_code = addr.postal_code,
    ex.state = addr.state,
    ex.timezone = addr.timezone;

-- Fill in missing addresses from contribution_tracking
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
  contact_hash varchar(32),
  first_name varchar(128),
  last_name varchar(128),
  preferred_language varchar(12),
  email varchar(255),
  opted_in tinyint(1),

  -- Lifetime contribution statistics
  has_recurred_donation tinyint(1),
  highest_usd_amount decimal(20,2),
  highest_native_amount decimal(20,2),
  highest_native_currency varchar(3),
  highest_donation_date datetime,
  lifetime_usd_total decimal(20,2),
  donation_count int,

  -- Aggregate contribution statistics
  total_2014 decimal(20,2) not null default 0,
  total_2015 decimal(20,2) not null default 0,
  total_2016 decimal(20,2) not null default 0,
  total_2017 decimal(20,2) not null default 0,
  total_2018 decimal(20,2) not null default 0,
  total_2019 decimal(20,2) not null default 0,
  total_2020 decimal(20,2) not null default 0,

    -- Endowment stats ----
  endowment_last_donation_date datetime null,
  endowment_first_donation_date datetime null,
  endowment_number_donations decimal(20,2) not null default 0,

  -- Latest contribution statistics
  latest_currency varchar(3),
  latest_currency_symbol varchar(8),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,
  first_donation_date datetime,

  -- Address information
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128),
  timezone varchar(8),

  CONSTRAINT sp_email UNIQUE (email),
  CONSTRAINT sp_contact_id UNIQUE (contact_id)
) COLLATE 'utf8_unicode_ci';

-- Move the data from the staging table into the persistent one
-- (12 minutes)
INSERT INTO silverpop_export (
  id,contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in,
  has_recurred_donation,highest_usd_amount,highest_native_amount,
  highest_native_currency,highest_donation_date,lifetime_usd_total,donation_count,
  latest_currency,latest_currency_symbol,latest_native_amount,latest_usd_amount,
  latest_donation, first_donation_date,city,country,state,postal_code,timezone,
  total_2014, total_2015, total_2016, total_2017,
  total_2018, total_2019, total_2020, endowment_last_donation_date, endowment_first_donation_date, endowment_number_donations)
SELECT id,contact_id,contact_hash,first_name,last_name,preferred_language,email,opted_in,
  has_recurred_donation,highest_usd_amount,highest_native_amount,
  highest_native_currency,highest_donation_date,lifetime_usd_total,donation_count,
  latest_currency,latest_currency_symbol,latest_native_amount,latest_usd_amount,
  latest_donation,first_donation_date,city,country,state,postal_code,timezone,
  total_2014, total_2015, total_2016, total_2017,
  total_2018, total_2019, total_2020, endowment_last_donation_date, endowment_first_donation_date,
  endowment_number_donations
FROM silverpop_export_staging
WHERE opted_out=0
ON DUPLICATE KEY UPDATE email = silverpop_export.email;

-- Create a nice view to export from
CREATE OR REPLACE VIEW silverpop_export_view AS
  SELECT
    contact_id ContactID,
    contact_hash,
    email,
    IFNULL(first_name, '') firstname,
    IFNULL(last_name, '') lastname,
    IFNULL(country, 'XX') country,
    state,
    postal_code,
    timezone,
    SUBSTRING(preferred_language, 1, 2) IsoLang,
    IF(has_recurred_donation, 'YES', 'NO') has_recurred_donation,
    CASE WHEN opted_in IS NULL THEN '' ELSE IF(opted_in,'YES','NO') END AS latest_optin_response,
    highest_usd_amount,
    highest_native_amount,
    highest_native_currency,
    IFNULL(DATE_FORMAT(highest_donation_date, '%m/%d/%Y'), '') highest_donation_date,
    lifetime_usd_total,
    IFNULL(DATE_FORMAT(latest_donation, '%m/%d/%Y'), '') latest_donation_date,
    latest_usd_amount,
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
    endowment_number_donations
  FROM silverpop_export;
