-- Updates the silverpop_export table

SET autocommit = 1;

DROP TABLE IF EXISTS silverpop_export_dedupe_email;
DROP TABLE IF EXISTS silverpop_export_dedupe_contact;
DROP TABLE IF EXISTS silverpop_export_stat;

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

  -- Latest contribution statistics
  last_ctid int unsigned,
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,

  -- Address information
  city varchar(128),
  country varchar(2),
  postal_code varchar(128),
  tzoffset float,

  -- Unsubcribe hash
  unsub_hash varchar(255),

  INDEX spex_contact_id (contact_id),
  INDEX spex_email (email),
  INDEX spex_city (city),
  INDEX spex_country (country),
  INDEX spex_postal (postal_code),
  INDEX spex_opted_out (opted_out)
) COLLATE 'utf8_unicode_ci';

-- Populate, or append to, the storage table all contacts that
-- have an email address. ID is civicrm_email.id which allows us to
-- retain work we've already done across runs.
INSERT INTO silverpop_export
  (id, contact_id, email, first_name, last_name, preferred_language, opted_out)
  SELECT
    e.id, e.contact_id, e.email, c.first_name, c.last_name,
    IF(SUBSTRING(c.preferred_language, 1, 1) = '_', 'en', SUBSTRING(c.preferred_language, 1, 2)),
    (c.is_deleted OR c.is_opt_out OR c.do_not_mail)
  FROM civicrm.civicrm_email e
  LEFT JOIN civicrm.civicrm_contact c ON e.contact_id = c.id
  WHERE
    e.email IS NOT NULL AND e.email != ''
  ON DUPLICATE KEY UPDATE
    email = e.email,
    first_name = c.first_name,
    last_name = c.last_name,
    preferred_language = IF(SUBSTRING(c.preferred_language, 1, 1) = '_', 'en', SUBSTRING(c.preferred_language, 1, 2)),
    opted_out = (c.is_deleted OR c.is_opt_out OR c.do_not_mail);

-- Populate data from contribution tracking; because that's fairly
-- reliable. Do this before deduplication so we can attempt to make
-- intelligent fallbacks in case of null data
UPDATE
    silverpop_export ex,
    civicrm.civicrm_contribution ct,
    drupal.contribution_tracking dct
  SET
    ex.preferred_language = dct.language
  WHERE
    ex.contact_id = ct.contact_id AND
    dct.contribution_id = ct.id AND
    dct.language IS NOT NULL;

UPDATE
    silverpop_export ex,
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
     FROM silverpop_export
       FORCE INDEX (spex_email)
       GROUP BY email
       HAVING count(*) > 1;

-- We pull in language/country from the parent table so that we
-- can preserve them and not propogate nulls
UPDATE silverpop_export_dedupe_email exde, silverpop_export ex
  SET
    exde.preferred_language = ex.preferred_language
  WHERE
    ex.email = exde.email AND
    ex.preferred_language IS NOT NULL;

UPDATE silverpop_export_dedupe_email exde, silverpop_export ex
  SET
    exde.country = ex.country
  WHERE
    ex.email = exde.email AND
    ex.country IS NOT NULL;

DELETE silverpop_export FROM silverpop_export, silverpop_export_dedupe_email
  WHERE
    silverpop_export.email = silverpop_export_dedupe_email.email AND
    silverpop_export.id != silverpop_export_dedupe_email.maxid;

UPDATE silverpop_export ex, silverpop_export_dedupe_email exde
  SET
    ex.opted_out = exde.opted_out,
    ex.preferred_language = exde.preferred_language,
    ex.country = exde.country
  WHERE
    exde.maxid = ex.id;

-- Deduplicate rows that have the same contact ID because they'll
-- generate the same result (~120 rows)
CREATE TABLE silverpop_export_dedupe_contact (
  id int PRIMARY KEY AUTO_INCREMENT,
  contact_id int,
  maxid int,
  opted_out tinyint(1),

  INDEX spexdc_optedout (opted_out)
) COLLATE 'utf8_unicode_ci';
  
INSERT INTO silverpop_export_dedupe_contact (contact_id, maxid, opted_out)
  SELECT contact_id, max(id) maxid, max(opted_out) opted_out FROM silverpop_export
    FORCE INDEX (spex_contact_id)
  GROUP BY contact_id
  HAVING count(*) > 1;

DELETE silverpop_export FROM silverpop_export, silverpop_export_dedupe_contact
  WHERE
    silverpop_export.contact_id = silverpop_export_dedupe_contact.contact_id AND
    silverpop_export.id != silverpop_export_dedupe_contact.maxid;

UPDATE silverpop_export ex, silverpop_export_dedupe_contact dc
  SET ex.opted_out = 1
  WHERE
    dc.opted_out = 1 AND dc.maxid = ex.id;

-- Create an aggregate table from a full contribution table scan
CREATE TABLE silverpop_export_stat (
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

  INDEX spexs_email (email)
) COLLATE 'utf8_unicode_ci';

INSERT INTO silverpop_export_stat
  (email, exid, max_ctid, max_amount_usd, total_usd, cnt_total, has_recurred_donation,
    cnt_2006, cnt_2007, cnt_2008, cnt_2009, cnt_2010, cnt_2011, cnt_2012, cnt_2013)
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
    SUM(IF('2012-07-1' <= ct.receive_date AND ct.receive_date <	 '2013-07-01', 1, 0)),
    SUM(IF('2013-07-1' <= ct.receive_date AND ct.receive_date < '2014-07-01', 1, 0))
  FROM civicrm.civicrm_email e FORCE INDEX(UI_email)
  JOIN silverpop_export ex ON e.email=ex.email
  JOIN civicrm.civicrm_contribution ct ON e.contact_id=ct.contact_id
  GROUP BY e.email;

UPDATE silverpop_export ex, silverpop_export_stat exs
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
    ex.is_2013_donor = IF(exs.cnt_2013 > 0, 1, 0)
  WHERE
    ex.id = exs.exid;

-- Populate information about the most recent contribution
UPDATE silverpop_export ex, civicrm.civicrm_contribution ct
SET
  latest_currency = SUBSTRING(ct.source, 1, 3),
  latest_native_amount = CONVERT(SUBSTRING(ct.source, 5), decimal(20,2)),
  latest_usd_amount = ct.total_amount,
  latest_donation = ct.receive_date
WHERE
  ex.last_ctid = ct.id AND
  ex.opted_out = 0;

-- Remove contacts who apparently have no contributions
DELETE FROM silverpop_export
  WHERE
    silverpop_export.latest_donation IS NULL AND
    silverpop_export.opted_out = 0;

-- Join on civicrm address where we do not already have a geolocated
-- address from contribution tracking
UPDATE silverpop_export ex, civicrm.civicrm_address addr, civicrm.civicrm_country ctry
  SET
    ex.city = addr.city,
    ex.country = ctry.iso_code,
    ex.postal_code = addr.postal_code
  WHERE
    ex.country IS NULL AND
    ex.tzoffset IS NULL AND
    ex.contact_id = addr.contact_id AND
    addr.country_id = ctry.id AND
    ex.opted_out = 0;

-- And now updated by civicrm address where we have a country but no
-- city from contribution tracking; the countries must match
UPDATE silverpop_export ex, civicrm.civicrm_address addr, civicrm.civicrm_country ctry
  SET
    ex.city = addr.city,
    ex.postal_code = addr.postal_code
  WHERE
    ex.country = ctry.iso_code AND
    ex.city IS NULL AND
    ex.tzoffset IS NULL AND
    ex.contact_id = addr.contact_id AND
    addr.country_id = ctry.id AND
    ex.opted_out = 0;

-- Reconstruct the donors likely language from their country if it
-- exists from a table of major language to country.
UPDATE silverpop_export ex, silverpop_countrylangs cl
  SET ex.preferred_language = cl.lang
  WHERE
    ex.country IS NOT NULL AND
    ex.preferred_language IS NULL AND
    ex.tzoffset IS NULL AND
    ex.country = cl.country AND
    ex.opted_out = 0;

-- Lookup timezone by country and post code -- for countries that span
-- multiple timezones.
UPDATE silverpop_export ex, dev_geonames.geonames g, dev_geonames.altnames a, dev_geonames.timezones tz
  SET ex.tzoffset = tz.offset
  WHERE
    ex.opted_out = 0 AND
    ex.tzoffset is NULL AND
    ex.postal_code IS NOT NULL AND
    ex.country IN ('FR', 'US', 'RU', 'AU', 'GB', 'CA', 'NZ', 'BR', 'ID', 'MX', 'PT', 'ES') AND
    a.format='post' AND
    ex.country = g.country_code AND
    a.altname = ex.postal_code AND
    a.geonameid = g.geonameid AND
    tz.tzid=g.tzid;

-- Lookup timezones by country (mostly for those that do not have
-- multiple timezones.)
UPDATE
  silverpop_export ex,
  (SELECT g.country_code country_code, tz.offset offset
    FROM dev_geonames.geonames g, dev_geonames.timezones tz 
    WHERE g.tzid=tz.tzid 
    GROUP BY g.country_code
  ) tz
  SET ex.tzoffset = tz.offset
  WHERE
    ex.opted_out = 0 AND
    ex.tzoffset is NULL AND
    tz.country_code=ex.country;
    
-- If we have no TZ information; set it to UTC
UPDATE silverpop_export ex
  SET ex.tzoffset = 0
  WHERE ex.tzoffset is NULL AND ex.opted_out = 0;
  
-- Normalize the data prior to final export
UPDATE silverpop_export SET preferred_language='en' WHERE preferred_language IS NULL;
UPDATE silverpop_export SET
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
    latest_currency = 'USD',
    latest_native_amount = 0,
    latest_usd_amount = 0,
    latest_donation = NOW(),
    has_recurred_donation = 0
  WHERE donation_count IS NULL AND opted_out = 0;
UPDATE silverpop_export SET country='US' where country IS NULL AND opted_out = 0;
