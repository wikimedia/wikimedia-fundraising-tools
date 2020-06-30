# The goal is not to have to drop this each time in future....
DROP TABLE IF EXISTS silverpop_export_staging;
DROP TABLE IF EXISTS silverpop_export_latest;
DROP TABLE IF EXISTS silverpop_export_highest;
DROP TABLE IF EXISTS silverpop_export_dedupe_email;
DROP TABLE IF EXISTS silverpop_export_stat;
DROP TABLE IF EXISTS silverpop_export_address;
DROP TABLE IF EXISTS silverpop_export;
DROP TABLE IF EXISTS `silverpop_export_matching_gift`;
DROP TABLE IF EXISTS silverpop_email_map;
DROP TABLE IF EXISTS silverpop_missing_countries;
DROP TABLE IF EXISTS silverpop_endowment_latest;
DROP TABLE IF EXISTS silverpop_endowment_highest;
DROP TABLE IF EXISTS silverpop_has_recur;
# We are no longer dropping and re-creating this table - just adding to it
# eventually this file might play a different schema re-set role
# so commenting for now.
#DROP TABLE IF EXISTS silverpop_excluded;
