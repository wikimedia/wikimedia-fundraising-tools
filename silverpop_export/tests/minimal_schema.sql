-- TODO: Do something SQLy to make *sure* we're not in a real database.

drop table if exists civicrm_email;
create table civicrm_email (
    id int(10) unsigned auto_increment primary key,
    contact_id int(10) unsigned,
    email varchar(254) COLLATE utf8mb4_unicode_ci,
    is_primary tinyint(4) default '1',
    on_hold tinyint(4) default '0',
    key UI_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_deleted_email;
create table civicrm_deleted_email (
    id int(10) unsigned primary key
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_contact;
create table civicrm_contact (
    id int(10) unsigned auto_increment primary key,
    contact_type varchar(64) default 'Individual',
    do_not_email tinyint(4) default '0',
    do_not_phone tinyint(4) default '0',
    do_not_mail tinyint(4) default '0',
    do_not_sms tinyint(4) default '0',
    do_not_trade tinyint(4) default '1',
    is_opt_out tinyint(4) default '0',
    preferred_language varchar(32) COLLATE utf8mb4_unicode_ci,
    hash varchar(32) COLLATE utf8mb4_unicode_ci,
    first_name varchar(64) COLLATE utf8mb4_unicode_ci,
    middle_name varchar(64) COLLATE utf8mb4_unicode_ci,
    last_name varchar(64) COLLATE utf8mb4_unicode_ci,
    organization_name varchar(64) COLLATE utf8mb4_unicode_ci,
    is_deleted tinyint(4) default '0',
    gender_id tinyint(4),
    birth_date datetime default NULL,
    modified_date datetime default NULL,
    employer_id int(10) unsigned DEFAULT NULL,
    email_greeting_display varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_currency;
create table civicrm_currency (
    id int(10) unsigned auto_increment primary key,
    name varchar(64) COLLATE utf8mb4_unicode_ci,
    symbol varchar(8) COLLATE utf8mb4_unicode_ci,
    key UI_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into civicrm_currency (name, symbol)
values
    ('USD', '$'),
    ('CAD', '$'),
    ('GBP', '£'),
    ('DZD', NULL);

drop table if exists wmf_donor;
CREATE TABLE `wmf_donor`
(
    `id`                            int(10) unsigned,
    `entity_id`                     int(10) unsigned,
    `last_donation_date`            datetime                             DEFAULT NULL,
    `last_donation_currency`        varchar(255)                         DEFAULT NULL,
    `last_donation_amount`          decimal(20, 2)                       DEFAULT '0.00',
    `last_donation_usd`             decimal(20, 2)                       DEFAULT '0.00',
    `lifetime_usd_total`            decimal(20, 2)                       DEFAULT '0.00',
    `total_2006_2007`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2007_2008`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2008_2009`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2009_2010`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2010_2011`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2011_2012`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2012_2013`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2013_2014`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2014_2015`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2015_2016`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2016_2017`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2017_2018`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2018_2019`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2019_2020`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2020_2021`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2021_2022`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2022_2023`               decimal(20, 2)                       DEFAULT '0.00',
    `total_2023_2024`               decimal(20, 2)                       DEFAULT '0.00',
    `endowment_last_donation_date`  datetime                             DEFAULT NULL,
    `first_donation_date`           datetime                             DEFAULT NULL,
    `endowment_first_donation_date` datetime                             DEFAULT NULL,
    `first_donation_usd`            decimal(20, 2)                       DEFAULT '0.00',
    `lifetime_including_endowment`  decimal(20, 2)                       DEFAULT '0.00',
    `endowment_lifetime_usd_total`  decimal(20, 2)                       DEFAULT '0.00',
    `number_donations`              int(11)                              DEFAULT '0',
    `endowment_number_donations`    int(11)                              DEFAULT '0',
    `largest_donation`              decimal(20, 2)                       DEFAULT '0.00',
    `endowment_largest_donation`    decimal(20, 2)                       DEFAULT '0.00',
    `date_of_largest_donation`      datetime                             DEFAULT NULL,
    `total_2006`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2007`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2008`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2009`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2010`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2011`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2012`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2013`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2014`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2015`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2016`                    decimal(20, 2)                       DEFAULT '0.00',
    `total_2017`                    decimal(20, 2)                       DEFAULT '0.00',
    `change_2017_2018`              double                               DEFAULT '0',
    `total_2018`                    decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2018`          decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2018_2019`     decimal(20, 2)                       DEFAULT '0.00',
    `change_2018_2019`              double                               DEFAULT '0',
    `total_2019`                    decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2019`          decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2019_2020`     decimal(20, 2)                       DEFAULT '0.00',
    `change_2019_2020`              double                               DEFAULT '0',
    `total_2020`                    decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2020`          decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2020_2021`     decimal(20, 2)                       DEFAULT '0.00',
    `change_2020_2021`              double                               DEFAULT '0',
    `total_2021`                    decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2021`          decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2021_2022`     decimal(20, 2)                       DEFAULT '0.00',
    `change_2021_2022`              double                               DEFAULT '0',
    `total_2022`                    decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2022`          decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2022_2023`     decimal(20, 2)                       DEFAULT '0.00',
    `change_2022_2023`              double                               DEFAULT '0',
    `total_2023`                    decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2023`          decimal(20, 2)                       DEFAULT '0.00',
    `endowment_total_2023_2022`     decimal(20, 2)                       DEFAULT '0.00',
    `change_2023_2024`              double                               DEFAULT '0'

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_value_1_communication_4;
create table civicrm_value_1_communication_4 (
    id int(10) unsigned,
    entity_id int(10) unsigned,
    do_not_solicit tinyint(4),
    opt_in tinyint(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_contribution;
create table civicrm_contribution (
    id int(10) unsigned,
    contact_id int(10) unsigned,
    contribution_recur_id int(10) unsigned,
    receive_date datetime,
    total_amount decimal(20,2),
    trxn_id varchar(255) COLLATE utf8mb4_unicode_ci,
    contribution_status_id int(10) unsigned,
    financial_type_id int(10) unsigned,
    KEY `received_date` (`receive_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_contribution_recur;
CREATE TABLE civicrm_contribution_recur
(
    id int(10) unsigned,
    contact_id int(10) unsigned NOT NULL COMMENT 'Foreign key to civicrm_contact.id.',
    amount decimal(20, 2) NOT NULL COMMENT 'Amount to be contributed or charged each recurrence.',
    currency varchar(3) NOT NULL DEFAULT 'USD',
    contribution_status_id int(10) unsigned DEFAULT '1',
    payment_processor_id int(10) NULL,
    end_date datetime,
    start_date datetime,
    cancel_date datetime
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


drop table if exists civicrm_address;
create table civicrm_address
(
  id INT(10) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  contact_id INT(10) UNSIGNED,
  is_primary TINYINT(4),
  city VARCHAR(64) COLLATE utf8mb4_unicode_ci,
  postal_code VARCHAR(64) COLLATE utf8mb4_unicode_ci,
  country_id INT(10) UNSIGNED,
  state_province_id INT(10) UNSIGNED,
  timezone VARCHAR(8) COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_country;
create table civicrm_country (
    id int(10) unsigned,
    iso_code char(2) COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_state_province;
create table civicrm_state_province (
  id int(10) unsigned,
  name varchar(64) COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists wmf_contribution_extra;
create table wmf_contribution_extra (
    entity_id int(10) unsigned,
    original_amount decimal(20,2),
    original_currency varchar(255) COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists contribution_tracking;
create table contribution_tracking (
    contribution_id int(10) unsigned,
    country varchar(2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists log_civicrm_email;
create table log_civicrm_email (
    id int(10) unsigned,
    contact_id int(10) unsigned,
    email varchar(254) COLLATE utf8mb4_unicode_ci,
    log_date datetime default NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_uf_match;
create table civicrm_uf_match (
    uf_name varchar(128) COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

drop table if exists civicrm_value_1_prospect_5;
create table civicrm_value_1_prospect_5
(
    `id`                              int(10) unsigned,
    `entity_id`                       int(10) unsigned,
    `income_range`                    varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `charitable_contributions_decile` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `disc_income_decile`              varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `subject_area_interest`           varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `university_affiliation`          varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `estimated_net_worth_144`         varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `endowment_stage_169`             varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `net_worth_170`                   varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `family_composition_173`          varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `occupation_175`                  varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS civicrm_value_matching_gift;
CREATE TABLE `civicrm_value_matching_gift` (
    `id` INT(10) UNSIGNED NOT NULL,
    `entity_id` INT(10) UNSIGNED NOT NULL,
    `matching_gifts_provider_id` VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `matching_gifts_provider_info_url` VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `name_from_matching_gift_db` VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `guide_url` VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `online_form_url` VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `minimum_gift_matched_usd` DECIMAL(20 , 2 ) DEFAULT NULL,
    `match_policy_last_updated` DATETIME DEFAULT NULL,
    `suppress_from_employer_field` TINYINT(4) DEFAULT '0',
    `subsidiaries` VARCHAR(5000) -- horrible hack to make tests work! https://stackoverflow.com/questions/31468080/the-used-table-type-does-not-support-blob-text-columns
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS civicrm_relationship;
CREATE TABLE `civicrm_relationship` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `contact_id_a` int(10) unsigned NOT NULL,
  `contact_id_b` int(10) unsigned NOT NULL,
  `relationship_type_id` int(10) unsigned NOT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `is_active` tinyint(4) DEFAULT 1,
  `description` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_permission_a_b` tinyint(4) DEFAULT 0,
  `is_permission_b_a` tinyint(4) DEFAULT 0,
  `case_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FK_civicrm_relationship_contact_id_a` (`contact_id_a`),
  KEY `FK_civicrm_relationship_contact_id_b` (`contact_id_b`),
  KEY `FK_civicrm_relationship_relationship_type_id` (`relationship_type_id`),
  KEY `FK_civicrm_relationship_case_id` (`case_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS `civicrm_relationship_type`;
CREATE TABLE `civicrm_relationship_type` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name_a_b` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `label_a_b` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name_b_a` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `label_b_a` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_type_a` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_type_b` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_sub_type_a` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_sub_type_b` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_reserved` tinyint(4) DEFAULT NULL,
  `is_active` tinyint(4) DEFAULT 1,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UI_name_a_b` (`name_a_b`),
  UNIQUE KEY `UI_name_b_a` (`name_b_a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS civicrm_value_relationship_metadata;
CREATE TABLE `civicrm_value_relationship_metadata` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `entity_id` int(10) unsigned NOT NULL,
  `provided_by_donor` tinyint(4) DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entity_id` (`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS civicrm_activity;
CREATE TABLE `civicrm_activity` (
    `id` int(10) unsigned NOT NULL,
    `activity_type_id` int(10) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS civicrm_activity_contact;
CREATE TABLE `civicrm_activity_contact` (
    `contact_id` int(10) unsigned NOT NULL,
    `activity_id` int(10) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS civicrm_payment_processor;
CREATE TABLE `civicrm_payment_processor` (
    `id` int(10) unsigned NOT NULL,
    `name` varchar(64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;