-- Drop tables that are build incrementally to facilitate a rebuild.
-- This is used for cleaning up when running tests.
DROP TABLE IF EXISTS silverpop_excluded;
DROP TABLE IF EXISTS silverpop_export_staging;
DROP TABLE IF EXISTS silverpop_export_stat;
DROP TABLE IF EXISTS silverpop_export_latest;
DROP TABLE IF EXISTS silverpop_export_highest;
DROP TABLE IF EXISTS silverpop_export;
DROP TABLE IF EXISTS silverpop_endowment_latest;
DROP TABLE IF EXISTS silverpop_endowment_highest;
DROP TABLE IF EXISTS silverpop_has_recur;
DROP TABLE IF EXISTS silverpop_export_checksum_email;