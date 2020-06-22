-- Collect email addresses which should be excluded for various reasons, such as:
-- * Exclude non-primary addresses
-- * Exclude any "former residence" email addresses.
-- * Exclude addresses dropped during contact merge.
-- We grab ALL addresses from the logs to start, then after we've figured out
-- which of the addresses on the include list are good, we remove them from
-- this table.
-- Same no-op update trick as with silverpop_export_latest
-- 44 min 38.01 sec
INSERT INTO silverpop_excluded (email)
SELECT email
FROM log_civicrm.log_civicrm_email e
     -- Ignore addresses created after the last address we picked
     -- up in the staging table select query above, so we don't
     -- opt-out contacts created since then.
WHERE id <= (SELECT MAX(id) FROM silverpop_export_staging)
ON DUPLICATE KEY UPDATE email = silverpop_excluded.email;

-- Remove all the known-good addresses from the suppression list.
DELETE silverpop_excluded
FROM silverpop_excluded
  LEFT JOIN silverpop_export_staging s
  ON s.email = silverpop_excluded.email
WHERE s.opted_out = 0
  AND (s.opted_in IS NULL OR s.opted_in = 1);

-- We don't want to suppress emails of Civi users.
-- Conveniently, the account name is the email address in
-- in the table that associates contacts with accounts.
DELETE silverpop_excluded
FROM silverpop_excluded
  JOIN civicrm.civicrm_uf_match m
  ON m.uf_name = silverpop_excluded.email;
