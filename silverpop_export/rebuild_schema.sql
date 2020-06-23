CREATE TABLE IF NOT EXISTS silverpop_export_staging(
    id int unsigned PRIMARY KEY,  -- This is actually civicrm_email.id

    -- General information about the contact
    contact_id int unsigned,
    modified_date datetime null,
    contact_hash varchar(32),
    first_name varchar(128),
    last_name varchar(128),
    preferred_language varchar(12),
    email varchar(255),
    opted_out tinyint(1),
    opted_in tinyint(1),
    employer_id int unsigned,
    employer_name varchar(255),

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
    latest_donation datetime null,
    first_donation_date datetime null,
    highest_donation_date datetime null,

    -- Address information
    city varchar(128),
    country varchar(2),
    state varchar(64),
    postal_code varchar(128),

    INDEX spex_contact_id (contact_id),
    INDEX spex_email (email),
    INDEX spex_country (country),
    INDEX spex_opted_out (opted_out),
    INDEX spex_modified_date(modified_date),
    INDEX spex_id(id)
) COLLATE 'utf8_unicode_ci';


CREATE TABLE IF NOT EXISTS silverpop_export_latest(
  email varchar(255) PRIMARY KEY,
  latest_currency varchar(3),
  latest_currency_symbol varchar(8),
  latest_native_amount decimal(20,2),
  latest_donation datetime
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS silverpop_excluded(
  id int AUTO_INCREMENT PRIMARY KEY,
  email varchar(255),

   INDEX sx_email (email),
   CONSTRAINT sx_email_u UNIQUE (email)
) COLLATE 'utf8_unicode_ci' AUTO_INCREMENT=1;

CREATE TABLE silverpop_export_highest(
  email varchar(255) PRIMARY KEY,
  highest_native_currency varchar(3),
  highest_native_amount decimal(20,2),
  highest_usd_amount decimal(20,2),
  highest_donation_date datetime
) COLLATE 'utf8_unicode_ci';


CREATE TABLE silverpop_export_stat (
   email varchar(255) PRIMARY KEY,
   exid INT,
   has_recurred_donation tinyint(1) not null default 0,
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

CREATE TABLE silverpop_export_address (
  email varchar(255) PRIMARY KEY,
  city varchar(128),
  country varchar(2),
  state varchar(64),
  postal_code varchar(128)
) COLLATE 'utf8_unicode_ci';

CREATE TABLE IF NOT EXISTS `silverpop_export_matching_gift` (
  `id` int(10) unsigned,
  `name` varchar(255),
  `matching_gifts_provider_info_url` varchar(255),
  `guide_url` varchar(255),
  `online_form_url` varchar(255) ,
  `minimum_gift_matched_usd` decimal(20,2),
  `match_policy_last_updated` datetime,
  `subsidiaries` varchar(5000), -- horrible hack to make tests work! https://stackoverflow.com/questions/31468080/the-used-table-type-does-not-support-blob-text-columns
  INDEX company_id (`id`)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

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
    employer_id int unsigned,
    employer_name varchar(255),

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
    latest_donation datetime,
    first_donation_date datetime,

    -- Address information
    city varchar(128),
    country varchar(2),
    state varchar(64),
    postal_code varchar(128),

    CONSTRAINT sp_email UNIQUE (email),
    CONSTRAINT sp_contact_id UNIQUE (contact_id)
) COLLATE 'utf8_unicode_ci';
