-- This file manages the generation of the silverpop_excluded table
-- which is then uploaded to Silverpop as a csv as the master suppression list
-- In normal practice this list is only ever added to. It takes a manual intervention
-- (by the email team) to purge it.
-- Although we upload the entire list each night it would be OK to upload new entries.

# Default offset - we can maybe pass this in from python.
# I think 2 (2 days) is probably the right value but a higher value for now
#reduces the risk a script fails and we don't notice.
# temporarily set to 14 days to catch any missed since we updated.
SET @offSetInDays = 14;

-- There are basically 2 steps to this -
-- 1) create/ augment the list of possible emails to suppress
-- 2) delete all valid emails.

-- 1: Creating the list - we are looking to :
-- Collect email addresses which should be excluded for various reasons, such as:
-- * Exclude non-primary addresses
-- * Exclude any "former residence" email addresses.
-- * Exclude addresses dropped during contact merge.
-- ALL addresses from the logs should be considered which we do by adding them
-- to the silverpop_excluded. When we run this job on a regular basis
-- we are interested in re-evaluating the ones that have possibly changed.
-- They could change as a result of
--   - the email changing - in this case the triggers on civicrm_email
--     will update civicrm_contact.modified_date
--   - the contact's preferences changing - in this case the triggers on contact
--     or the custom tables will update the civicrm_contact.modifed_date
--   - a contact merge. When a contact is merged the modified_date on the new_contact
--    is updated by the email change and on the old contact by virtue of the deleted
--   flag being set.
--   - a contact being hard-deleted from the database. Since we don't hard delete
--     until they have been soft-deleted for aa long time this should be handled before it
--     gets to this stage.
--   - privacy deletes - these are deleted from the log_civicrm_email table so
--    this process can't address these
-- So it seems safe to assume all all relevant contacts would have a civicrm_contact
-- record with a recent modified_date. However, for belt & braces we also add from
-- recently modified log_civicrm_email entries.

-- We exclude all contacts with an id greater than the value in the latest value
-- in the silverpop_export_staging table as we are still using that table to calculate our opt
-- in/ opt out.... if we include emails not yet updated in that table
-- they would have no opted in match in that table and would wind up being opted out.
-- @todo look at not using this table & just doing more remove queries - can
-- we avoid the big OR if we do that.

-- In my tests this takes about 15 seconds with an interval of 7 days and under 1 second for 2 days
-- note that this reflects the number of changed rows on those days - the 2 days in question were low traffic.
-- we could do these updates often -eg. hourly but would need to track last updated.
INSERT INTO silverpop_excluded (email)
SELECT DISTINCT e.email
FROM log_civicrm.log_civicrm_email e
LEFT JOIN civicrm.civicrm_contact c ON c.id = e.contact_id
-- see comment block for long discussion of this WHERE
WHERE e.id <= (SELECT MAX(id) FROM silverpop_export_staging)
  AND c.modified_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
ON DUPLICATE KEY UPDATE email = silverpop_excluded.email;

-- In the test we check the possibility of updated orphaned emails.
-- I am not sure if it's valid - per the discussion above this would only
-- apply on a permanent and we soft delete first. But it's tested &
-- the query is not expensive so...
-- Query OK, 1058 rows affected (2.32 sec)
INSERT INTO silverpop_excluded (email)
SELECT DISTINCT e.email
FROM log_civicrm.log_civicrm_email e
WHERE e.id <= (SELECT MAX(id) FROM silverpop_export_staging)
AND e.log_date > DATE_SUB(NOW(), INTERVAL @offSetInDays DAY)
ON DUPLICATE KEY UPDATE email = silverpop_excluded.email;

-- Remove all the known-good addresses from the suppression list.
-- seconds to process.
-- We use the summary table which has calculated values for the overall
-- values for the email.
DELETE silverpop_excluded
FROM silverpop_excluded
  LEFT JOIN silverpop_email_map s
  ON s.email = silverpop_excluded.email
WHERE s.opted_out = 0
  AND s.opted_in = 1;

-- We don't want to suppress emails of Civi users.
-- Conveniently, the account name is the email address in
-- in the table that associates contacts with accounts.
DELETE silverpop_excluded
FROM silverpop_excluded
  JOIN civicrm.civicrm_uf_match m
  ON m.uf_name = silverpop_excluded.email;

CREATE OR REPLACE VIEW silverpop_excluded_utf8 as
  SELECT id, CONVERT(email USING utf8) FROM silverpop_excluded;
