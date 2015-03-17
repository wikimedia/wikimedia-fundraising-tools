-- Updates the silverpop_export table

SET autocommit = 1;

DROP TABLE IF EXISTS temp_silverpop_export;
DROP TABLE IF EXISTS temp_silverpop_export_dedupe_email;
DROP TABLE IF EXISTS temp_silverpop_export_stat;

CREATE TEMPORARY TABLE IF NOT EXISTS temp_silverpop_export(
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
  is_2006_donor tinyint(1),
  is_2007_donor tinyint(1),
  is_2008_donor tinyint(1),
  is_2009_donor tinyint(1),
  is_2010_donor tinyint(1),
  is_2011_donor tinyint(1),
  is_2012_donor tinyint(1),
  is_2013_donor tinyint(1),
  is_2014_donor tinyint(1),

  -- Latest contribution statistics
  last_ctid int unsigned,
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,

  -- Address information
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128),

  -- Unsubcribe hash
  unsub_hash varchar(255),

  INDEX spex_contact_id (contact_id),
  INDEX spex_email (email),
  INDEX spex_city (city),
  INDEX spex_country (country),
  INDEX spex_opted_out (opted_out)
) COLLATE 'utf8_unicode_ci';

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id.
INSERT INTO temp_silverpop_export
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
    AND c.is_deleted = 0;

-- Populate data from contribution tracking; because that's fairly
-- reliable. Do this before deduplication so we can attempt to make
-- intelligent fallbacks in case of null data
UPDATE
    temp_silverpop_export ex,
    civicrm.civicrm_contribution ct,
    drupal.contribution_tracking dct
  SET
    ex.preferred_language = dct.language
  WHERE
    ex.contact_id = ct.contact_id AND
    dct.contribution_id = ct.id AND
    dct.language IS NOT NULL;

UPDATE
    temp_silverpop_export ex,
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
CREATE TEMPORARY TABLE temp_silverpop_export_dedupe_email (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email varchar(255),
  maxid int,
  preferred_language varchar(5),
  country varchar(2),
  opted_out tinyint(1),

  INDEX spexde_email (email)
) COLLATE 'utf8_unicode_ci';

INSERT INTO temp_silverpop_export_dedupe_email (email, maxid, opted_out)
   SELECT email, max(id) maxid, max(opted_out) opted_out
     FROM temp_silverpop_export
       FORCE INDEX (spex_email)
       GROUP BY email
       HAVING count(*) > 1;

-- We pull in language/country from the parent table so that we
-- can preserve them and not propogate nulls
UPDATE temp_silverpop_export_dedupe_email exde, temp_silverpop_export ex
  SET
    exde.preferred_language = ex.preferred_language
  WHERE
    ex.email = exde.email AND
    ex.preferred_language IS NOT NULL;

UPDATE temp_silverpop_export_dedupe_email exde, temp_silverpop_export ex
  SET
    exde.country = ex.country
  WHERE
    ex.email = exde.email AND
    ex.country IS NOT NULL;

DELETE temp_silverpop_export FROM temp_silverpop_export, temp_silverpop_export_dedupe_email
  WHERE
    temp_silverpop_export.email = temp_silverpop_export_dedupe_email.email AND
    temp_silverpop_export.id != temp_silverpop_export_dedupe_email.maxid;

UPDATE temp_silverpop_export ex, temp_silverpop_export_dedupe_email exde
  SET
    ex.opted_out = exde.opted_out,
    ex.preferred_language = exde.preferred_language,
    ex.country = exde.country
  WHERE
    exde.maxid = ex.id;

-- Create an aggregate table from a full contribution table scan
CREATE TEMPORARY TABLE temp_silverpop_export_stat (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email varchar(255),
  exid INT,                         -- STEP 5
  max_ctid INT,                     -- STEP 5
  max_amount_ctid INT,              -- STEP 5
  max_amount_usd decimal(20,2),     -- STEP 5
  max_amount_currency varchar(3),   -- STEP 5
  has_recurred_donation tinyint(1),
  total_usd decimal(20,2),          -- STEP 5
  cnt_total int unsigned,           -- STEP 5
  cnt_2006 int unsigned,            -- STEP 5
  cnt_2007 int unsigned,
  cnt_2008 int unsigned,
  cnt_2009 int unsigned,
  cnt_2010 int unsigned,
  cnt_2011 int unsigned,
  cnt_2012 int unsigned,
  cnt_2013 int unsigned,
  cnt_2014 int unsigned,

  INDEX spexs_email (email)
) COLLATE 'utf8_unicode_ci';

INSERT INTO temp_silverpop_export_stat
  (email, exid, max_ctid, max_amount_usd, total_usd, cnt_total, has_recurred_donation,
    cnt_2006, cnt_2007, cnt_2008, cnt_2009, cnt_2010, cnt_2011, cnt_2012, cnt_2013, cnt_2014)
  SELECT
    e.email, ex.id, MAX(ct.id), MAX(ct.total_amount), SUM(ct.total_amount),
    count(*),
    MAX(IF(SUBSTRING(ct.trxn_id, 1, 9) = 'RECURRING', 1, 0)),
    SUM(IF('2006-07-1' <= ct.receive_date AND ct.receive_date < '2007-07-01', 1, 0)),
    SUM(IF('2007-07-1' <= ct.receive_date AND ct.receive_date < '2008-07-01', 1, 0)),
    SUM(IF('2008-07-1' <= ct.receive_date AND ct.receive_date < '2009-07-01', 1, 0)),
    SUM(IF('2009-07-1' <= ct.receive_date AND ct.receive_date < '2010-07-01', 1, 0)),
    SUM(IF('2010-07-1' <= ct.receive_date AND ct.receive_date < '2011-07-01', 1, 0)),
    SUM(IF('2011-07-1' <= ct.receive_date AND ct.receive_date < '2012-07-01', 1, 0)),
    SUM(IF('2012-07-1' <= ct.receive_date AND ct.receive_date <	'2013-07-01', 1, 0)),
    SUM(IF('2013-07-1' <= ct.receive_date AND ct.receive_date < '2014-07-01', 1, 0)),
    SUM(IF('2014-07-1' <= ct.receive_date AND ct.receive_date < '2015-07-01', 1, 0))
  FROM civicrm.civicrm_email e FORCE INDEX(UI_email)
  JOIN temp_silverpop_export ex ON e.email=ex.email
  JOIN civicrm.civicrm_contribution ct ON e.contact_id=ct.contact_id
  GROUP BY e.email;

UPDATE temp_silverpop_export ex, temp_silverpop_export_stat exs
  SET
    ex.last_ctid = exs.max_ctid,
    ex.highest_usd_amount = exs.max_amount_usd,
    ex.lifetime_usd_total = exs.total_usd,
    ex.donation_count = exs.cnt_total,
    ex.has_recurred_donation = IF(exs.has_recurred_donation > 0, 1, 0),
    ex.is_2006_donor = IF(exs.cnt_2006 > 0, 1, 0),
    ex.is_2007_donor = IF(exs.cnt_2007 > 0, 1, 0),
    ex.is_2008_donor = IF(exs.cnt_2008 > 0, 1, 0),
    ex.is_2009_donor = IF(exs.cnt_2009 > 0, 1, 0),
    ex.is_2010_donor = IF(exs.cnt_2010 > 0, 1, 0),
    ex.is_2011_donor = IF(exs.cnt_2011 > 0, 1, 0),
    ex.is_2012_donor = IF(exs.cnt_2012 > 0, 1, 0),
    ex.is_2013_donor = IF(exs.cnt_2013 > 0, 1, 0),
    ex.is_2014_donor = IF(exs.cnt_2014 > 0, 1, 0)
  WHERE
    ex.id = exs.exid;

-- Populate information about the most recent contribution
UPDATE temp_silverpop_export ex, civicrm.civicrm_contribution ct
SET
  latest_currency = SUBSTRING(ct.source, 1, 3),
  latest_native_amount = CONVERT(SUBSTRING(ct.source, 5), decimal(20,2)),
  latest_usd_amount = ct.total_amount,
  latest_donation = ct.receive_date
WHERE
  ex.last_ctid = ct.id;

-- Remove contacts who apparently have no contributions
-- Leave opted out non-contributors so we don't spam anyone
DELETE FROM temp_silverpop_export
  WHERE
    temp_silverpop_export.latest_donation IS NULL AND
    temp_silverpop_export.opted_out = 0;

-- Join on civicrm address where we do not already have a geolocated
-- address from contribution tracking
UPDATE temp_silverpop_export ex
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
-- city from contribution tracking; the countries must match
UPDATE temp_silverpop_export ex
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
UPDATE temp_silverpop_export ex, silverpop_countrylangs cl
  SET ex.preferred_language = cl.lang
  WHERE
    ex.country IS NOT NULL AND
    ex.preferred_language IS NULL AND
    ex.country = cl.country AND
    ex.opted_out = 0;

-- Normalize the data prior to final export
UPDATE temp_silverpop_export SET preferred_language='en' WHERE preferred_language IS NULL;
UPDATE temp_silverpop_export SET
    last_ctid = 0,
    highest_usd_amount = 0,
    lifetime_usd_total = 0,
    donation_count = 0,
    is_2007_donor = 0,
    is_2008_donor = 0,
    is_2009_donor = 0,
    is_2010_donor = 0,
    is_2011_donor = 0,
    is_2012_donor = 0,
    is_2013_donor = 0,
    is_2014_donor = 0,
    latest_currency = 'USD',
    latest_native_amount = 0,
    latest_usd_amount = 0,
    latest_donation = NOW(),
    has_recurred_donation = 0
  WHERE donation_count IS NULL AND opted_out = 0;
UPDATE temp_silverpop_export SET country='US' where country IS NULL AND opted_out = 0;

DROP TABLE IF EXISTS silverpop_export;

CREATE TABLE IF NOT EXISTS silverpop_export(
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
  is_2006_donor tinyint(1),
  is_2007_donor tinyint(1),
  is_2008_donor tinyint(1),
  is_2009_donor tinyint(1),
  is_2010_donor tinyint(1),
  is_2011_donor tinyint(1),
  is_2012_donor tinyint(1),
  is_2013_donor tinyint(1),
  is_2014_donor tinyint(1),

  -- Latest contribution statistics
  last_ctid int unsigned,
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,

  -- Address information
  city varchar(128),
  country varchar(2),
  state varchar(24),
  postal_code varchar(128),

  -- Unsubcribe hash
  unsub_hash varchar(255),

  INDEX rspex_contact_id (contact_id),
  INDEX rspex_email (email),
  INDEX rspex_city (city),
  INDEX rspex_country (country),
  INDEX rspex_postal (postal_code),
  INDEX rspex_opted_out (opted_out),
  INDEX rspex_is_2006_donor (is_2006_donor),
  INDEX rspex_is_2007_donor (is_2007_donor),
  INDEX rspex_is_2008_donor (is_2008_donor),
  INDEX rspex_is_2009_donor (is_2009_donor),
  INDEX rspex_is_2010_donor (is_2010_donor),
  INDEX rspex_is_2011_donor (is_2011_donor),
  INDEX rspex_is_2012_donor (is_2012_donor),
  INDEX rspex_is_2013_donor (is_2013_donor),
  INDEX rspex_is_2014_donor (is_2014_donor)
) COLLATE 'utf8_unicode_ci';

-- Move the data from the temp table into the persistent one
INSERT INTO silverpop_export (
  id,contact_id,first_name,last_name,preferred_language,email,opted_out,
  has_recurred_donation,highest_usd_amount,lifetime_usd_total,donation_count,
  is_2006_donor,is_2007_donor,is_2008_donor,is_2009_donor,is_2010_donor,
  is_2011_donor,is_2012_donor,is_2013_donor,is_2014_donor,last_ctid,
  latest_currency,latest_native_amount,latest_usd_amount,latest_donation,
  city,country,state,postal_code,unsub_hash )
SELECT id,contact_id,first_name,last_name,preferred_language,email,opted_out,
  has_recurred_donation,highest_usd_amount,lifetime_usd_total,donation_count,
  is_2006_donor,is_2007_donor,is_2008_donor,is_2009_donor,is_2010_donor,
  is_2011_donor,is_2012_donor,is_2013_donor,is_2014_donor,last_ctid,
  latest_currency,latest_native_amount,latest_usd_amount,latest_donation,
  city,country,state,postal_code,unsub_hash
FROM temp_silverpop_export;

-- Create a nice view to export from
CREATE OR REPLACE VIEW silverpop_export_view AS
  SELECT
    contact_id ContactID,
    email,
    IFNULL(first_name, '') firstname,
    IFNULL(last_name, '') lastname,
    last_ctid ContributionID,
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
    donation_count,
    IF(is_2006_donor, 'YES', 'NO') is_2006_donor,
    IF(is_2007_donor, 'YES', 'NO') is_2007_donor,
    IF(is_2008_donor, 'YES', 'NO') is_2008_donor,
    IF(is_2009_donor, 'YES', 'NO') is_2009_donor,
    IF(is_2010_donor, 'YES', 'NO') is_2010_donor,
    IF(is_2011_donor, 'YES', 'NO') is_2011_donor,
    IF(is_2012_donor, 'YES', 'NO') is_2012_donor,
    IF(is_2013_donor, 'YES', 'NO') is_2013_donor,
    IF(is_2014_donor, 'YES', 'NO') is_2014_donor,
    unsub_hash
  FROM silverpop_export
  WHERE opted_out=0;

