from process.globals import config
from database import db

class ReviewQueue(object):
    @staticmethod
    def addMatch(job_id, oldId, newId, action, match):
        db.get_db(config.drupal_schema).execute("""
            INSERT INTO donor_review_queue
                SET
                    job_id = %(job_id)s,
                    old_id = %(old_id)s,
                    new_id = %(new_id)s,
                    action_id = %(action_id)s,
                    match_description = %(match)s
            """, {
                'job_id': job_id,
                'old_id': oldId,
                'new_id': newId,
                'action_id': action.id,
                'match': match,
            })

    @staticmethod
    def tag(contact_id, tag):
        db.get_db(config.civicrm_schema).execute("""
            INSERT IGNORE INTO civicrm_entity_tag
                SET
                    entity_table = 'civicrm_contact',
                    entity_id = %(contact_id)s,
                    tag_id = %(tag_id)s
            """, {
                'contact_id': contact_id,
                'tag_id': tag.id,
            })
