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
-- This is not used in the final output, but it is used in determining if the row is the most recent
  all_funds_latest_donation_date DATETIME NULL,

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
  INDEX address_id (address_id),
  INDEX(email,all_funds_latest_donation_date, id, address_id, preferred_language, opted_out, opted_in)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_email_map`
(
  `email` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `master_email_id` INT(16) NOT NULL,
  `address_id` INT(16) DEFAULT NULL,
  `preferred_language` VARCHAR(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `opted_out` TINYINT(1) DEFAULT NULL,
  `opted_in` TINYINT(1) DEFAULT NULL,
  `modified_date` DATETIME NULL,
  KEY `master_email_id` (`master_email_id`),
  KEY `address_id` (`address_id`),
  KEY `email` (`email`)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_export_latest
(
  email VARCHAR(255) PRIMARY KEY,
  latest_currency VARCHAR(3),
  latest_currency_symbol VARCHAR(8),
  latest_native_amount DECIMAL(20, 2),
  latest_payment_method VARCHAR(64),
  latest_donation_source VARCHAR(64)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_endowment_latest` (
  `email` varchar(255)  PRIMARY KEY,
  `endowment_latest_currency` VARCHAR(8),
  `endowment_latest_currency_symbol` VARCHAR(8),
  `endowment_latest_native_amount` DECIMAL(20, 2),
  `endowment_latest_payment_method` VARCHAR(64),
  `endowment_latest_donation_source` VARCHAR(64),
  KEY `email` (`email`)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_endowment_highest` (
 `email` varchar(255) PRIMARY KEY,
 `endowment_highest_donation_date` DATETIME,
 `endowment_highest_native_currency` VARCHAR(8),
 `endowment_highest_native_amount` DECIMAL(20, 2)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_excluded
(
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255),

  INDEX sx_email (email),
  CONSTRAINT sx_email_u UNIQUE (email)
) COLLATE 'utf8mb4_unicode_ci'
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS silverpop_export_highest
(
  email VARCHAR(255) PRIMARY KEY,
  highest_native_currency VARCHAR(3),
  highest_native_amount DECIMAL(20, 2),
  highest_usd_amount DECIMAL(20, 2),
  highest_donation_date DATETIME
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_latest_direct_mail
(
  email VARCHAR(255) PRIMARY KEY,
  appeal VARCHAR(255)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_export_stat
(
  email VARCHAR(255) PRIMARY KEY,
  all_funds_latest_donation_date DATETIME,
  all_funds_total_2018_2019 DECIMAL(20, 2),
  all_funds_total_2019_2020 DECIMAL(20, 2),
  all_funds_total_2020_2021 DECIMAL(20, 2),
  all_funds_total_2021_2022 DECIMAL(20, 2),
  all_funds_total_2022_2023 DECIMAL(20, 2),
  all_funds_total_2023_2024 DECIMAL(20, 2),
  all_funds_total_2024_2025 DECIMAL(20, 2),
  all_funds_total_2025_2026 DECIMAL(20, 2),
  foundation_lifetime_usd_total DECIMAL(20, 2),
  foundation_donation_count INT UNSIGNED NOT NULL DEFAULT 0,
  foundation_first_donation_date DATETIME,
  foundation_last_donation_date DATETIME,
  foundation_highest_usd_amount  DECIMAL(20, 2),
-- Aggregate contribution statistics
  donor_segment_id DECIMAL(20, 2),
  donor_status_bin INT(10) UNSIGNED,
  endowment_last_donation_date DATETIME NULL,
  endowment_first_donation_date DATETIME NULL,
  endowment_number_donations INT UNSIGNED NOT NULL DEFAULT 0,
  endowment_highest_usd_amount  DECIMAL(20, 2),
  INDEX(all_funds_latest_donation_date),
  INDEX(endowment_last_donation_date),
  INDEX(endowment_highest_usd_amount)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_export
(
  id INT UNSIGNED PRIMARY KEY, -- This is actually civicrm_email.id
  modified_date DATETIME NULL,
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
  direct_mail_latest_appeal VARCHAR(255),

-- Matching gift employer data
  matching_gifts_provider_info_url VARCHAR(255),
  guide_url VARCHAR(255),
  online_form_url VARCHAR(255),

-- recurring statistics
  foundation_has_recurred_donation TINYINT(1),
  foundation_has_active_recurring_donation TINYINT(1),
  foundation_recurring_first_donation_date DATETIME,
  foundation_recurring_latest_donation_date DATETIME,
  foundation_recurring_active_count TINYINT UNSIGNED, -- First donor with 256 active recurrings wins a prize
  foundation_recurring_latest_contribution_recur_id INT(10),
  recurring_has_upgrade_activity TINYINT(1),

-- Lifetime contribution statistics
  foundation_highest_usd_amount DECIMAL(20, 2),
  foundation_highest_native_amount DECIMAL(20, 2),
  foundation_highest_native_currency VARCHAR(3),
  foundation_highest_donation_date DATETIME,
  lifetime_usd_total DECIMAL(20, 2),
  donation_count INT UNSIGNED NOT NULL DEFAULT 0,

-- aggregate contribution amounts
  all_funds_total_2018_2019 DECIMAL(20, 2),
  all_funds_total_2019_2020 DECIMAL(20, 2),
  all_funds_total_2020_2021 DECIMAL(20, 2),
  all_funds_total_2021_2022 DECIMAL(20, 2),
  all_funds_total_2022_2023 DECIMAL(20, 2),
  all_funds_total_2023_2024 DECIMAL(20, 2),
  all_funds_total_2024_2025 DECIMAL(20, 2),
  all_funds_total_2025_2026 DECIMAL(20, 2),

-- Endowment stats ----
  endowment_last_donation_date DATETIME NULL,
  endowment_first_donation_date DATETIME NULL,
  endowment_number_donations INT UNSIGNED NOT NULL DEFAULT 0,
  endowment_highest_usd_amount  DECIMAL(20, 2),

-- Latest contribution statistics
  foundation_latest_currency VARCHAR(3),
  foundation_latest_currency_symbol VARCHAR(8),
  foundation_latest_native_amount DECIMAL(20, 2),
  foundation_last_donation_date DATETIME,
  foundation_first_donation_date DATETIME,

-- Address information
  city VARCHAR(128),
  country VARCHAR(2),
  state VARCHAR(64),
  postal_code VARCHAR(128),

  donor_segment_id INT(11),
  donor_status_bin INT(10) UNSIGNED,

  INDEX spex_modified_date (modified_date),
  CONSTRAINT sp_email UNIQUE (email),
  CONSTRAINT sp_contact_id UNIQUE (contact_id)
) COLLATE 'utf8mb4_unicode_ci';

-- contacts and countries where they are not present in the contact record but ARE present in the
-- contribution tracking but not the Civi contact record (around 400k)
CREATE TABLE IF NOT EXISTS `silverpop_missing_countries`
(
  `contact_id` INT(10) UNSIGNED NOT NULL COMMENT 'FK to Contact ID',
  `country` VARCHAR(2) DEFAULT NULL,
  `preferred_language` VARCHAR(32) DEFAULT NULL,
  KEY `contact_id` (`contact_id`),
  KEY `country` (`country`)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_has_recur` (
 `email` VARCHAR(255) PRIMARY KEY,
 `foundation_has_recurred_donation` int(1) NOT NULL,
 `foundation_has_active_recurring_donation` TINYINT(1),
 `foundation_recurring_latest_donation_date` DATETIME,
 `foundation_recurring_first_donation_date` DATETIME,
 `foundation_recurring_active_count` TINYINT UNSIGNED,
 `foundation_recurring_latest_contribution_recur_id` INT(10),
 `recurring_has_upgrade_activity` TINYINT(1),
 `most_recent_cancel_date` DATETIME
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS `cancel_reason` (
 `email` VARCHAR(255) PRIMARY KEY,
 `most_recent_cancel_reason` VARCHAR(255)
) COLLATE 'utf8mb4_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_export_checksum_email`
(
 `email` VARCHAR(255) PRIMARY KEY,
 `checksum` VARCHAR(255)
) COLLATE 'utf8mb4_unicode_ci';
