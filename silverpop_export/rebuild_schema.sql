CREATE TABLE IF NOT EXISTS silverpop_export_staging
(
  id INT UNSIGNED PRIMARY KEY, -- This is actually civicrm_email.id

-- General information about the contact
  contact_id INT UNSIGNED,
  modified_date DATETIME NULL,
  contact_hash VARCHAR(32),
  first_name VARCHAR(128),
  last_name VARCHAR(128),
  preferred_language VARCHAR(12),
  email VARCHAR(255),
  opted_out TINYINT(1),
  opted_in TINYINT(1),
  employer_id INT UNSIGNED,
  employer_name VARCHAR(255),

-- Lifetime contribution statistics
  has_recurred_donation TINYINT(1) NOT NULL DEFAULT 0,
  highest_usd_amount DECIMAL(20, 2) NOT NULL DEFAULT 0,
  highest_native_amount DECIMAL(20, 2) NOT NULL DEFAULT 0,
  highest_native_currency VARCHAR(3) NOT NULL DEFAULT '',

  -- Latest contribution statistics
  latest_currency VARCHAR(3) NOT NULL DEFAULT '',
  latest_currency_symbol VARCHAR(8) NOT NULL DEFAULT '',
  latest_native_amount DECIMAL(20, 2) NOT NULL DEFAULT 0,
  latest_donation DATETIME NULL,
  highest_donation_date DATETIME NULL,

-- Address information
  address_id INT(16),
  city VARCHAR(128),
  country VARCHAR(2),
  state VARCHAR(64),
  postal_code VARCHAR(128),

  INDEX spex_contact_id (contact_id),
  INDEX spex_email (email),
  INDEX spex_country (country),
  INDEX spex_opted_out (opted_out),
  INDEX spex_modified_date (modified_date),
  INDEX spex_id (id),
  INDEX address_id (address_id)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE `silverpop_email_map`
(
  `email` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `master_email_id` INT(16) NOT NULL,
  `address_id` INT(16) DEFAULT NULL,
  `preferred_language` VARCHAR(12) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `opted_out` TINYINT(1) DEFAULT NULL,
  KEY `master_email_id` (`master_email_id`),
  KEY `address_id` (`address_id`),
  KEY `email` (`email`)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_export_latest
(
  email VARCHAR(255) PRIMARY KEY,
  latest_currency VARCHAR(3),
  latest_currency_symbol VARCHAR(8),
  latest_native_amount DECIMAL(20, 2),
  latest_donation DATETIME
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_endowment_latest` (
  `email` varchar(255)  PRIMARY KEY,
  `endowment_latest_currency` VARCHAR(8),
  `endowment_latest_native_amount` DECIMAL(20, 2),
  KEY `email` (`email`)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_endowment_highest` (
 `email` varchar(255) PRIMARY KEY,
 `endowment_highest_donation_date` DATETIME,
 `endowment_highest_native_currency` VARCHAR(8),
 `endowment_highest_native_amount` DECIMAL(20, 2)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_excluded
(
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255),

  INDEX sx_email (email),
  CONSTRAINT sx_email_u UNIQUE (email)
) COLLATE 'utf8_unicode_ci'
  AUTO_INCREMENT = 1;

CREATE TABLE silverpop_export_highest
(
  email VARCHAR(255) PRIMARY KEY,
  highest_native_currency VARCHAR(3),
  highest_native_amount DECIMAL(20, 2),
  highest_usd_amount DECIMAL(20, 2),
  highest_donation_date DATETIME
) COLLATE 'utf8_unicode_ci';


CREATE TABLE silverpop_export_stat
(
  email VARCHAR(255) PRIMARY KEY,
  exid INT,
  all_funds_latest_donation_date DATETIME,
  has_recurred_donation TINYINT(1) NOT NULL DEFAULT 0,
  foundation_lifetime_usd_total DECIMAL(20, 2),
  foundation_donation_count INT UNSIGNED,
  foundation_first_donation_date DATETIME,
  foundation_last_donation_date DATETIME,
  foundation_highest_usd_amount  DECIMAL(20, 2),
-- Aggregate contribution statistics
  foundation_total_2014 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2015 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2016 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2017 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2018 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2019 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2020 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  endowment_last_donation_date DATETIME NULL,
  endowment_first_donation_date DATETIME NULL,
  endowment_number_donations DECIMAL(20, 2) NOT NULL DEFAULT 0,
  endowment_highest_usd_amount  DECIMAL(20, 2),
  INDEX stat_exid (exid),
  INDEX(all_funds_latest_donation_date),
  INDEX(endowment_last_donation_date),
  INDEX(endowment_highest_usd_amount)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE silverpop_export_address
(
  email VARCHAR(255) PRIMARY KEY,
  city VARCHAR(128),
  country VARCHAR(2),
  state VARCHAR(64),
  postal_code VARCHAR(128)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_export_matching_gift`
(
  `id` INT(10) UNSIGNED,
  `name` VARCHAR(255),
  `matching_gifts_provider_info_url` VARCHAR(255),
  `guide_url` VARCHAR(255),
  `online_form_url` VARCHAR(255),
  `minimum_gift_matched_usd` DECIMAL(20, 2),
  `match_policy_last_updated` DATETIME,
  `subsidiaries` VARCHAR(5000), -- horrible hack to make tests work! https://stackoverflow.com/questions/31468080/the-used-table-type-does-not-support-blob-text-columns
  INDEX company_id (`id`)
) DEFAULT CHARSET = utf8
  COLLATE = utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS silverpop_export
(
  id INT UNSIGNED PRIMARY KEY, -- This is actually civicrm_email.id

-- General information about the contact
  contact_id INT UNSIGNED,
  contact_hash VARCHAR(32),
  first_name VARCHAR(128),
  last_name VARCHAR(128),
  preferred_language VARCHAR(12),
  email VARCHAR(255),
  opted_in TINYINT(1),
  employer_id INT UNSIGNED,
  employer_name VARCHAR(255),

-- Lifetime contribution statistics
  has_recurred_donation TINYINT(1),
  highest_usd_amount DECIMAL(20, 2),
  highest_native_amount DECIMAL(20, 2),
  highest_native_currency VARCHAR(3),
  highest_donation_date DATETIME,
  lifetime_usd_total DECIMAL(20, 2),
  donation_count INT,

-- Aggregate contribution statistics
  foundation_total_2014 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2015 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2016 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2017 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2018 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2019 DECIMAL(20, 2) NOT NULL DEFAULT 0,
  foundation_total_2020 DECIMAL(20, 2) NOT NULL DEFAULT 0,

-- Endowment stats ----
  endowment_last_donation_date DATETIME NULL,
  endowment_first_donation_date DATETIME NULL,
  endowment_number_donations DECIMAL(20, 2) NOT NULL DEFAULT 0,

-- Latest contribution statistics
  latest_currency VARCHAR(3),
  latest_currency_symbol VARCHAR(8),
  latest_native_amount DECIMAL(20, 2),
  latest_donation DATETIME,
  foundation_first_donation_date DATETIME,

-- Address information
  city VARCHAR(128),
  country VARCHAR(2),
  state VARCHAR(64),
  postal_code VARCHAR(128),

  CONSTRAINT sp_email UNIQUE (email),
  CONSTRAINT sp_contact_id UNIQUE (contact_id)
) COLLATE 'utf8_unicode_ci';

-- contacts and countries where they are not present in the contact record but ARE present in the
-- contribution tracking but not the Civi contact record (around 400k)
CREATE TABLE `silverpop_missing_countries`
(
  `contact_id` INT(10) UNSIGNED NOT NULL COMMENT 'FK to Contact ID',
  `country` VARCHAR(2) DEFAULT NULL,
  `preferred_language` VARCHAR(32) DEFAULT NULL,
  KEY `contact_id` (`contact_id`),
  KEY `country` (`country`)
) COLLATE 'utf8_unicode_ci';
