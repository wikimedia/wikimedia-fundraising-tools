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
create table wmf_donor (
    id int(10) unsigned,
    entity_id int(10) unsigned,
    do_not_solicit tinyint(4),
    is_2006_donor tinyint(4),
    is_2007_donor tinyint(4),
    is_2008_donor tinyint(4),
    is_2009_donor tinyint(4),
    is_2010_donor tinyint(4),
    is_2011_donor tinyint(4),
    is_2012_donor tinyint(4),
    is_2013_donor tinyint(4),
    is_2014_donor tinyint(4),
    is_2015_donor tinyint(4),
    is_2016_donor tinyint(4),
    is_2017_donor tinyint(4),
    is_2018_donor tinyint(4),
    is_2019_donor tinyint(4),
    is_2020_donor tinyint(4),
    is_2021_donor tinyint(4),
    is_2022_donor tinyint(4),
    is_2023_donor tinyint(4),
    is_2024_donor tinyint(4),
    is_2025_donor tinyint(4),
    last_donation_date datetime,
    last_donation_currency varchar(255) COLLATE utf8_unicode_ci,
    last_donation_amount decimal(20,2),
    last_donation_usd decimal(20,2),
    lifetime_usd_total decimal(20,2)
);

drop table if exists civicrm_contribution;
create table civicrm_contribution (
    id int(10) unsigned,
    contact_id int(10) unsigned,
    receive_date datetime,
    total_amount decimal(20,2),
    trxn_id varchar(255) COLLATE utf8_unicode_ci,
    contribution_status_id int(10) unsigned
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
    email varchar(254) COLLATE utf8_unicode_ci
);

drop table if exists civicrm_uf_match;
create table civicrm_uf_match (
    uf_name varchar(128) COLLATE utf8_unicode_ci
);
