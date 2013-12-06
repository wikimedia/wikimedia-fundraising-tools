SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

DROP TABLE IF EXISTS silverpop_export;
DROP TABLE IF EXISTS silverpop_unsubscribe_export;
CREATE TABLE silverpop_export(
  id int unsigned PRIMARY KEY AUTO_INCREMENT,
  
  -- Step 1 exported fields
  contact_id int unsigned,
  first_name varchar(128),
  last_name varchar(128),
  preferred_language varchar(5),
  email varchar(255),
  opted_out tinyint(1),
  
  -- Step 5 lifetime statistics
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
  
  -- Step 6 latest contribution
  last_ctid int unsigned,
  latest_currency varchar(3),
  latest_native_amount decimal(20,2),
  latest_usd_amount decimal(20,2),
  latest_donation datetime,
  
  -- Step 7 Address information
  city varchar(128),
  country varchar(2),
  postal_code varchar(128),
  
  -- Step 8 Geonames lookup of timezone
  tzoffset float,
  
  -- Step 10 Unsubcribe hash generation
  unsub_hash varchar(255)
);
CREATE INDEX spex_contact_id ON silverpop_export(contact_id);
CREATE INDEX spex_email ON silverpop_export(email);
CREATE INDEX spex_city ON silverpop_export(city);
CREATE INDEX spex_country ON silverpop_export(country);
CREATE INDEX spex_postal ON silverpop_export(postal_code);
CREATE INDEX spex_opted_out ON silverpop_export(opted_out);

-- STEP 1: Populate the temporary table with all contacts that have an
-- email address
INSERT INTO silverpop_export
  (contact_id, email, first_name, last_name, preferred_language, opted_out)
  SELECT
    e.contact_id, e.email, c.first_name, c.last_name,
    IF(SUBSTRING(c.preferred_language, 1, 1) = '_', 'en', SUBSTRING(c.preferred_language, 1, 2)),
    c.is_deleted OR c.is_opt_out
  FROM civicrm.civicrm_email e, civicrm.civicrm_contact c
  WHERE
    e.email IS NOT NULL AND e.email != '' AND
    e.contact_id = c.id;

-- STEP 2: Deduplicate rows that have the same email address, we will
-- have to merge in more data later, but this is >500k rows we're
-- getting rid of here which is more better than taking them all the way
-- through.
CREATE TABLE silverpop_export_dedupe_email
  (id INT PRIMARY KEY AUTO_INCREMENT, email varchar(255), maxid int, opted_out tinyint(1));

INSERT INTO silverpop_export_dedupe_email (email, maxid, opted_out)
   SELECT email, max(id) maxid, max(opted_out) opted_out FROM silverpop_export
     FORCE INDEX (spex_email)
     GROUP BY email
     HAVING count(*) > 1;

DELETE silverpop_export FROM silverpop_export, silverpop_export_dedupe_email
  WHERE
    silverpop_export.email = silverpop_export_dedupe_email.email AND
    silverpop_export.id != silverpop_export_dedupe_email.maxid;

UPDATE silverpop_export ex, silverpop_export_dedupe_email de
  SET ex.opted_out = 1
  WHERE
    de.opted_out = 1 AND de.maxid = ex.contact_id;

DROP TABLE silverpop_export_dedupe_email;

-- STEP 3: Deduplicate rows that have the same contact ID because they'll
-- generate the same result (> 50k rows)
CREATE TABLE silverpop_export_dedupe_contact
  (id int PRIMARY KEY AUTO_INCREMENT, contact_id int, maxid int, opted_out tinyint(1));
CREATE INDEX spexdc_optedout ON silverpop_export_dedupe_contact(opted_out);
  
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
    dc.opted_out = 1 AND dc.maxid = ex.contact_id;

DROP TABLE silverpop_export_dedupe_contact;

-- STEP 4 Was updating opt outs; but I've merged that into steps 2 and 3

-- STEP 5: Create an aggregate table from a full contribution table scan
DROP TABLE IF EXISTS silverpop_export_stat;
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
  cnt_total int,                    -- STEP 5
  cnt_2006 tinyint,                 -- STEP 5
  cnt_2007 tinyint,
  cnt_2008 tinyint,
  cnt_2009 tinyint,
  cnt_2010 tinyint,
  cnt_2011 tinyint,
  cnt_2012 tinyint,
  cnt_2013 tinyint
);

INSERT INTO silverpop_export_stat
  (email, exid, max_ctid, max_amount_usd, total_usd, cnt_total, has_recurred_donation,
    cnt_2006, cnt_2007, cnt_2008, cnt_2009, cnt_2010, cnt_2011, cnt_2012, cnt_2013)
  SELECT
    e.email, ex.id, MAX(ct.id), MAX(ct.total_amount), SUM(ct.total_amount),
    count(*),
    SUM(IF(SUBSTRING(ct.trxn_id, 1, 9) = 'RECURRING', 1, 0)),
    SUM(IF('2006-07-1' < ct.receive_date AND ct.receive_date < '2007-07-01', 1, 0)),
    SUM(IF('2007-07-1' < ct.receive_date AND ct.receive_date < '2008-07-01', 1, 0)),
    SUM(IF('2008-07-1' < ct.receive_date AND ct.receive_date < '2009-07-01', 1, 0)),
    SUM(IF('2009-07-1' < ct.receive_date AND ct.receive_date < '2010-07-01', 1, 0)),
    SUM(IF('2010-07-1' < ct.receive_date AND ct.receive_date < '2011-07-01', 1, 0)),
    SUM(IF('2011-07-1' < ct.receive_date AND ct.receive_date < '2012-07-01', 1, 0)),
    SUM(IF('2012-07-1' < ct.receive_date AND ct.receive_date < '2013-07-01', 1, 0)),
    SUM(IF('2013-07-1' < ct.receive_date AND ct.receive_date < '2014-07-01', 1, 0))
  FROM silverpop_export ex, civicrm.civicrm_email e, civicrm.civicrm_contribution ct
  WHERE e.email=ex.email AND e.contact_id=ct.contact_id AND ex.opted_out = 0
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

DROP TABLE silverpop_export_stat;
  
-- STEP 6: Populate information about the most recent contribution
UPDATE silverpop_export ex, civicrm.civicrm_contribution ct
SET
  latest_currency = SUBSTRING(ct.source, 1, 3),
  latest_native_amount = CONVERT(SUBSTRING(ct.source, 5), decimal(20,2)),
  latest_usd_amount = ct.total_amount,
  latest_donation = ct.receive_date
WHERE
  ex.last_ctid = ct.id AND
  ex.opted_out = 0;

-- STEP 7: Join on address
UPDATE silverpop_export ex, civicrm.civicrm_address addr, civicrm.civicrm_country ctry
  SET
    ex.city = addr.city,
    ex.country = ctry.iso_code,
    ex.postal_code = addr.postal_code
  WHERE ex.contact_id = addr.contact_id AND addr.country_id = ctry.id AND ex.opted_out = 0;

-- STEP 8: Geonames lookup of timezone
-- 8.1 Lookup by post code and country
UPDATE silverpop_export ex, geonames.geonames g, geonames.altnames a, geonames.timezones tz
  SET ex.tzoffset = tz.offset
  WHERE
    ex.opted_out = 0 AND
    ex.postal_code IS NOT NULL AND
    ex.country IN ('FR', 'US', 'RU', 'AU', 'GB', 'CA', 'NZ', 'BR', 'ID', 'MX', 'PT', 'ES') AND
    a.format='post' AND
    ex.country = g.country_code AND
    a.altname = ex.postal_code AND
    a.geonameid = g.geonameid AND
    tz.tzid=g.tzid;

-- 8.2 Otherwise just by country
UPDATE
  silverpop_export ex,
  (SELECT g.country_code country_code, tz.offset offset
    FROM geonames.geonames g, geonames.timezones tz 
    WHERE g.tzid=tz.tzid 
    GROUP BY g.country_code
  ) tz
  SET ex.tzoffset = tz.offset
  WHERE
    ex.opted_out = 0 AND
    ex.tzoffset is NULL AND
    tz.country_code=ex.country;
    
-- 8.3 And really otherwise, just set it to 0
UPDATE silverpop_export ex
  SET ex.tzoffset = 0
  WHERE ex.tzoffset is NULL AND ex.opted_out = 0;
  
-- STEP 9 Normalize some data
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
  WHERE donation_count IS NULL AND ex.opted_out = 0;
UPDATE silverpop_export SET country='US' where country IS NULL AND ex.opted_out = 0;

-- STEP 10 Create the unsub hash
UPDATE silverpop_export ex
SET unsub_hash = SHA1(CONCAT(last_ctid, email, XXX))
WHERE ex.opted_out = 0;
  
-- Export some random rows
-- Run something like this from the command line like so...
-- mysql -h db1008.eqiad.wmnet mwalker < silverpop_export.sql | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > out.csv
SELECT contact_id ContactID, email, first_name firstname, last_name lastname,
  last_ctid ContributionID, country, SUBSTRING(preferred_language, 1, 2) IsoLang,
  IF(has_recurred_donation, 'TRUE', 'FALSE') has_recurred_donation, 
  highest_usd_amount, lifetime_usd_total,
  DATE_FORMAT(latest_donation, '%m/%d/%Y') latest_donation_date,
  latest_usd_amount, latest_currency, latest_native_amount,
  tzoffset timezone, donation_count,
  IF(is_2006_donor, 'TRUE', 'FALSE') is_2006_donor, 
  IF(is_2007_donor, 'TRUE', 'FALSE') is_2007_donor, 
  IF(is_2008_donor, 'TRUE', 'FALSE') is_2008_donor, 
  IF(is_2009_donor, 'TRUE', 'FALSE') is_2009_donor, 
  IF(is_2010_donor, 'TRUE', 'FALSE') is_2010_donor,
  IF(is_2011_donor, 'TRUE', 'FALSE') is_2011_donor, 
  IF(is_2012_donor, 'TRUE', 'FALSE') is_2012_donor, 
  IF(is_2013_donor, 'TRUE', 'FALSE') is_2013_donor, 
  unsub_hash 
FROM silverpop_export AS r1 JOIN
  (SELECT (RAND() * (SELECT MAX(id) FROM silverpop_export)) AS id) AS r2
 WHERE r1.id >= r2.id
 ORDER BY r1.id ASC
 LIMIT 1000;
 
-- Also export the unsubscribes
SELECT email FROM silverpop_export WHERE opted_out=1 LIMIT 1000;
