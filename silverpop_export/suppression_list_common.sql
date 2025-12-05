-- We don't want to suppress emails of Civi users.
-- Conveniently, the account name is the email address in
-- in the table that associates contacts with accounts.
DELETE silverpop_excluded
FROM silverpop_excluded
  JOIN civicrm.civicrm_uf_match m
  ON m.uf_name = silverpop_excluded.email;

CREATE OR REPLACE VIEW silverpop_excluded_utf8 as
  SELECT email FROM silverpop_excluded;