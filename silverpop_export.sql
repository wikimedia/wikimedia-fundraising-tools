DROP TABLE IF EXISTS silverpop_export;
CREATE TABLE silverpop_export(
  id int unsigned PRIMARY KEY AUTO_INCREMENT,
  
  -- Step 1 exported fields
  contact_id int unsigned,
  first_name varchar(128),
  last_name varchar(128),
  preferred_language varchar(5),
  email varchar(255),
  
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
  city varchar(64),
  country varchar(2),
  postal_code varchar(16),
  
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

-- STEP 1: Populate the temporary table with all contacts that have an
-- email address
INSERT INTO silverpop_export
  (contact_id, email, first_name, last_name, preferred_language)
  SELECT
    e.contact_id, e.email, c.first_name, c.last_name,
    SUBSTRING(c.preferred_language, 1, 2)
  FROM civicrm.civicrm_email e, civicrm.civicrm_contact c
  WHERE
    e.email IS NOT NULL AND e.email != '' AND
    e.contact_id = c.id AND
    c.is_deleted = 0 AND c.is_opt_out = 0;

-- STEP 2: Deduplicate rows that have the same email address, we will
-- have to merge in more data later, but this is >500k rows we're
-- getting rid of here which is more better than taking them all the way
-- through.
CREATE TABLE silverpop_export_dedupe_email
  (id INT PRIMARY KEY AUTO_INCREMENT, email varchar(255), maxid int);

INSERT INTO silverpop_export_dedupe_email (email, maxid)
   SELECT email, max(id) maxid FROM silverpop_export
     FORCE INDEX (spex_email)
     GROUP BY email
     HAVING count(*) > 1;

DELETE silverpop_export FROM silverpop_export, silverpop_export_dedupe_email
  WHERE
    silverpop_export.email = silverpop_export_dedupe_email.email AND
    silverpop_export.id != silverpop_export_dedupe_email.maxid;
  
DROP TABLE silverpop_export_dedupe_email;

-- STEP 3: Deduplicate rows that have the same contact ID because they'll
-- generate the same result (> 50k rows)
CREATE TABLE silverpop_export_dedupe_contact
  (id int PRIMARY KEY AUTO_INCREMENT, contact_id int, maxid int);
  
INSERT INTO silverpop_export_dedupe_contact (contact_id, maxid)
  SELECT contact_id, max(id) maxid FROM silverpop_export
    FORCE INDEX (spex_contact_id)
  GROUP BY contact_id
  HAVING count(*) > 1;

DELETE silverpop_export FROM silverpop_export, silverpop_export_dedupe_contact
  WHERE
    silverpop_export.contact_id = silverpop_export_dedupe_contact.contact_id AND
    silverpop_export.id != silverpop_export_dedupe_contact.maxid;

DROP TABLE silverpop_export_dedupe_contact;

-- STEP 4 Update every email address with every contact and opt them out
DELETE silverpop_export ex
FROM silverpop_export ex, civicrm.civicrm_email e USE INDEX(UI_email), civicrm.civicrm_contact c
WHERE
  ex.email = e.email AND e.contact_id = c.id AND c.is_opt_out = 1;

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
  WHERE e.email=ex.email AND e.contact_id=ct.contact_id
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
  ex.last_ctid = ct.id;

-- STEP 7: Join on address
UPDATE silverpop_export ex, civicrm.civicrm_address addr, civicrm.civicrm_country ctry
  SET
    ex.city = addr.city,
    ex.country = ctry.iso_code,
    ex.postal_code = addr.postal_code
  WHERE ex.contact_id = addr.contact_id AND addr.country_id = ctry.id;

-- STEP 8: Geonames lookup of timezone
-- 8.1 Lookup by post code and country
UPDATE silverpop_export ex, geonames.geonames g, geonames.altnames a, geonames.timezones tz
  SET ex.tzoffset = tz.offset
  WHERE
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
    ex.tzoffset is NULL AND
    tz.country_code=ex.country;
    
-- 8.3 And really otherwise, just set it to 0
UPDATE silverpop_export ex
  SET ex.tzoffset = 0
  WHERE ex.tzoffset is NULL;
  
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
  WHERE donation_count IS NULL;
UPDATE silverpop_export SET country='US' where country IS NULL;

-- STEP 10 Create the unsub hash
UPDATE silverpop_export ex SET
  unsub_hash = SHA1(CONCAT(last_ctid, email, XXX));
  
-- Export some random rows
-- Run something like this from the command line like so...
-- mysql -h db1008.eqiad.wmnet -u pcoombe -p mwalker < query.sql | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > out.csv
SELECT contact_id ContactID, email, first_name firstname, last_name lastname,
  last_ctid ContributionID, country, 
  SUBSTRING(preferred_language, 1, 2) IsoLang, has_recurred_donation, highest_usd_amount,
  lifetime_usd_total, latest_donation latest_donation_date, latest_usd_amount,
  latest_currency, latest_native_amount, tzoffset timezone, donation_count,
  is_2006_donor, is_2007_donor, is_2008_donor, is_2009_donor, is_2010_donor,
  is_2011_donor, is_2012_donor, is_2013_donor, unsub_hash 
FROM silverpop_export AS r1 JOIN
  (SELECT (RAND() * (SELECT MAX(id) FROM silverpop_export)) AS id) AS r2
 WHERE r1.id >= r2.id
 ORDER BY r1.id ASC
 LIMIT 200;

