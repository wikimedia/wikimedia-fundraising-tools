-- TODO: Do something SQLy to make *sure* we're not in a real database.

drop table if exists civicrm_email;
create table civicrm_email (
    id int(10) unsigned auto_increment primary key,
    contact_id int(10) unsigned,
    email varchar(254) COLLATE utf8_unicode_ci,
    is_primary tinyint(4) default '1',
    on_hold tinyint(4) default '0',
    key UI_email (email)
);

drop table if exists civicrm_contact;
create table civicrm_contact (
    id int(10) unsigned auto_increment primary key,
    do_not_email tinyint(4) default '0',
    do_not_phone tinyint(4) default '0',
    do_not_mail tinyint(4) default '0',
    do_not_sms tinyint(4) default '0',
    do_not_trade tinyint(4) default '1',
    is_opt_out tinyint(4) default '0',
    preferred_language varchar(32) COLLATE utf8_unicode_ci,
    hash varchar(32) COLLATE utf8_unicode_ci,
    first_name varchar(64) COLLATE utf8_unicode_ci,
    middle_name varchar(64) COLLATE utf8_unicode_ci,
    last_name varchar(64) COLLATE utf8_unicode_ci,
    is_deleted tinyint(4) default '0'
);

drop table if exists civicrm_currency;
create table civicrm_currency (
    id int(10) unsigned auto_increment primary key,
    name varchar(64) COLLATE utf8_unicode_ci,
    symbol varchar(8) COLLATE utf8_unicode_ci,
    key UI_name (name)
);
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
    `change_2020_2021`              double                               DEFAULT '0'

);

drop table if exists civicrm_value_1_communication_4;
create table civicrm_value_1_communication_4 (
    id int(10) unsigned,
    entity_id int(10) unsigned,
    do_not_solicit tinyint(4),
    opt_in tinyint(1)
);

drop table if exists civicrm_contribution;
create table civicrm_contribution (
    id int(10) unsigned,
    contact_id int(10) unsigned,
    contribution_recur_id int(10) unsigned,
    receive_date datetime,
    total_amount decimal(20,2),
    trxn_id varchar(255) COLLATE utf8_unicode_ci,
    contribution_status_id int(10) unsigned,
    financial_type_id int(10) unsigned
);

drop table if exists civicrm_contribution_recur;
CREATE TABLE civicrm_contribution_recur
(
    id int(10) unsigned,
    contact_id int(10) unsigned NOT NULL COMMENT 'Foreign key to civicrm_contact.id.',
    amount decimal(20, 2)   NOT NULL COMMENT 'Amount to be contributed or charged each recurrence.',
    contribution_status_id int(10) unsigned DEFAULT '1'
);


drop table if exists civicrm_address;
create table civicrm_address (
    id int(10) unsigned auto_increment primary key,
    contact_id int(10) unsigned,
    is_primary tinyint(4),
    city varchar(64) COLLATE utf8_unicode_ci,
    postal_code varchar(64) COLLATE utf8_unicode_ci,
    country_id int(10) unsigned,
    state_province_id int(10) unsigned,
    timezone varchar(8) COLLATE utf8_unicode_ci
);

drop table if exists civicrm_country;
create table civicrm_country (
    id int(10) unsigned,
    iso_code char(2) COLLATE utf8_unicode_ci
);

drop table if exists civicrm_state_province;
create table civicrm_state_province (
  id int(10) unsigned,
  name varchar(64) COLLATE utf8_unicode_ci
);

drop table if exists wmf_contribution_extra;
create table wmf_contribution_extra (
    entity_id int(10) unsigned,
    original_amount decimal(20,2),
    original_currency varchar(255) COLLATE utf8_unicode_ci
);

drop table if exists contribution_tracking;
create table contribution_tracking (
    contribution_id int(10) unsigned,
    country varchar(2)
);

drop table if exists log_civicrm_email;
create table log_civicrm_email (
    id int(10) unsigned,
    email varchar(254) COLLATE utf8_unicode_ci
);

drop table if exists civicrm_uf_match;
create table civicrm_uf_match (
    uf_name varchar(128) COLLATE utf8_unicode_ci
);
