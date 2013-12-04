from process.logging import Logger as log
from process.globals import config
from database import db

class ReviewQueue(object):
    cached_tagging = True
    cached_tags = {}

    @staticmethod
    def addMatch(job_id, oldId, newId, action, match):
        #log.info("Found a match: {old} -> {new} : {match}".format(old=oldId, new=newId, match=match))
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
        if ReviewQueue.cached_tagging:
            if tag not in ReviewQueue.cached_tags:
                ReviewQueue.cached_tags[tag] = []

            ReviewQueue.cached_tags[tag].append(contact_id)
        else:
            ReviewQueue.tag_single(contact_id, tag)

    @staticmethod
    def commit():
        log.info("Committing tags...")
        for tag, contacts in ReviewQueue.cached_tags.items():
            log.info("Bulk tagging {num} contacts with tag <{tag}>".format(num=len(contacts), tag=tag.name))
            ReviewQueue.tag_many(contacts, tag)

    @staticmethod
    def tag_many(contacts, tag):
        sets = [ "('civicrm_contact', {contact_id}, {tag_id})".format(contact_id=contact_id, tag_id=tag.id)
            for contact_id in contacts ]
        values = ", ".join(sets)

        db.get_db(config.civicrm_schema).execute("""
            INSERT IGNORE INTO civicrm_entity_tag
                (entity_table, entity_id, tag_id)
            VALUES
                %s
        """ % values)

    @staticmethod
    def tag_single(contact_id, tag):
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
