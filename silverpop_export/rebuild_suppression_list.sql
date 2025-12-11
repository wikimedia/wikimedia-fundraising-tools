-- This file rebuilds the silverpop_excluded table from scratch, adding only emails that should not be sent mailings
-- It takes about 20 minutes to run

TRUNCATE TABLE silverpop_excluded;

-- Add all emails that have a contact who is opted out, opt in = no, do not solicit, do not email, or email is on hold
-- We add emails shared between contacts if one is opted out, etc
-- Deleted contacts are covered in the next query, so exclude them here because they might be opted out, etc and we don't care about this if they are deleted
-- 10900918 rows affected (4 min 31.516 sec)
INSERT INTO silverpop_excluded (email)
SELECT e.email
FROM civicrm.civicrm_email e
INNER JOIN civicrm.civicrm_contact c
	ON c.id = e.contact_id
	AND c.is_deleted = 0
LEFT JOIN civicrm.civicrm_value_1_communication_4 com
	ON com.entity_id = c.id
WHERE (
	e.on_hold <> 0
	OR c.is_opt_out = 1
	OR c.do_not_email = 1
	OR com.opt_in = 0
	OR com.do_not_solicit = 1
)
ON DUPLICATE KEY UPDATE email = VALUES(email);


-- Add all emails that are non-primary or are on a deleted contact that aren't a primary email for another non-deleted contact
-- 323759 rows affected (7 min 20.447 sec)
INSERT INTO silverpop_excluded (email)
SELECT e.email
FROM civicrm.civicrm_email e
INNER JOIN civicrm.civicrm_contact c
	ON c.id = e.contact_id
WHERE (
	e.is_primary = 0
	OR c.is_deleted = 1
)
AND NOT EXISTS (
	SELECT 1
	FROM civicrm.civicrm_email esub
	INNER JOIN civicrm.civicrm_contact csub
	    ON csub.id = esub.contact_id
    WHERE esub.email = e.email
	AND csub.is_deleted = 0
	AND esub.is_primary = 1
)
ON DUPLICATE KEY UPDATE email = VALUES(email);

-- Add all the emails that are in the email log table, but aren't in the current email table.
-- These are deleted emails, emails for deleted contacts or emails that have been changed, either way we don't want to email them.
-- 362267 rows affected (9 min 2.031 sec)
INSERT INTO silverpop_excluded (email)
SELECT log.email
FROM civicrm.log_civicrm_email log
WHERE NOT EXISTS (
    SELECT 1
    FROM civicrm.civicrm_email e
    WHERE e.email = log.email
)
AND log.email NOT LIKE ' %' -- Acoustic removes the space, so we would opt out emails that have been changed to remove the leading space
ON DUPLICATE KEY UPDATE email = VALUES(email);

