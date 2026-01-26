-- This file manages the generation of the silverpop_excluded table
-- which is then uploaded to Silverpop as a csv as the master suppression list and as an
-- identical second csv to opt out all the emails (for visibility / query-ability in Acoustic)
-- In normal practice this list is only ever added to.
-- If we want to remove contacts, we can use Omnicontact::create to re opt in and remove from MSL
-- or we can use Omnicontact::bulkReOptIn to do the same from a table of emails.
-- Although we upload the entire list each night it would be OK to upload new entries instead.
-- Note that this list can be purged and rebuilt with rebuild_suppression_list on our end,
-- but currently we cannot we must purge emails one by one as above from the Acoustic end.

-- There are basically 2 steps to this -
-- 1) create/ augment the list of possible emails to suppress
-- 2) delete all valid emails.

-- 1: Creating the list - we are looking to :
-- Collect email addresses which should be excluded for various reasons, such as:
-- * Exclude non-primary addresses,
--   where the same email is not primary for another non-deleted contact and not on hold
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
-- To make sure we capture emails for deleted contacts, we also add from
-- recently modified log_civicrm_email entries.

-- We exclude all contacts with an id greater than the value in the latest value
-- in the silverpop_export_staging table as we are still using that table to calculate our opt
-- in/ opt out.... if we include emails not yet updated in that table
-- they would have no opted in match in that table and would wind up being opted out.
-- @todo look at not using this table & just doing more remove queries - can
-- we avoid the big OR if we do that.
-- we could do these updates often -eg. hourly but would need to track last updated.
-- We can't use log_civicrm_email here because then we end up capturing emails that were
-- attached in the past to recently modified contacts, but aren't currently.
-- Query OK, 1471779 rows affected (1 min 35.897 sec) (for 7 days near year end, so high traffic)
INSERT INTO silverpop_excluded (email)
SELECT DISTINCT e.email
FROM civicrm.civicrm_email e
LEFT JOIN civicrm.civicrm_contact c ON c.id = e.contact_id
LEFT JOIN civicrm.civicrm_value_1_communication_4 com
  ON com.entity_id = c.id
-- see comment block for long discussion of this WHERE
WHERE e.id <= (SELECT MAX(id) FROM silverpop_export_staging)
  AND c.modified_date BETWEEN @startDate AND @endDate
  -- We don't want to exclude emails that are non-primary but shared with someone else
  -- as primary (unless on hold) because they won't be in silverpop_email_map
  -- because they are non-primary (unless the primary contact was also modified in the window).
  AND (e.is_primary = 1
    OR (
      e.is_primary = 0
      AND (
        e.on_hold <> 0
        OR NOT EXISTS (
          SELECT 1
          FROM civicrm.civicrm_email esub
          INNER JOIN civicrm.civicrm_contact csub
            ON csub.id = esub.contact_id
          WHERE esub.email = e.email AND esub.is_primary = 1 AND csub.is_deleted = 0
        )
      )
    )
  )
ON DUPLICATE KEY UPDATE email = silverpop_excluded.email;

-- In the test we check the possibility of updated orphaned emails
-- as well as for emails that are (exclusively) attached to deleted contacts.
-- Query OK, 783 rows affected (1.578 sec)
INSERT INTO silverpop_excluded (email)
SELECT DISTINCT e.email
FROM civicrm.log_civicrm_email e
LEFT JOIN civicrm.civicrm_email em ON em.email = e.email
  WHERE e.id <= (SELECT MAX(id) FROM silverpop_export_staging)
  AND e.log_date BETWEEN @startDate AND @endDate
  AND em.id IS NULL
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
