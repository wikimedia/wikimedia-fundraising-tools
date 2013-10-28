'''Optimized retrieval and in-memory storage of a small amount of information across many contacts.'''

from process.logging import Logger as log
from process.globals import config
from database import db

class ContactCache(object):
    def __init__(self, require_email=False, **kw):
        self.columns = []
        self.contacts = []
        self.require_email = require_email

    def isEmpty(self):
        return not self.contacts.empty()

    def fetch(self):
        '''Load a batch of contacts into the cache'''
        query = self.buildQuery()

        self.contacts = []
        result = db.get_db().execute(query)
        for row in result:
            name_components = []
            keys = ['first_name', 'middle_name', 'last_name', 'organization_name']

            for key in keys:
                if key in row and row[key]:
                    name_components.append(row[key])

            #TODO: consider some flatter structure:
            #self.contacts.append([
            #	row['id'],
            #	" ".join(name_components),
            #	row['email'],
            #])
            self.contacts.append({
                'id': row['id'],
                'name': " ".join(name_components),
                'email': row['email'],
            })

    def buildQuery(self):
        query = db.Query()
        query.columns.extend([
            "contact.id",
            "contact.first_name",
            "contact.middle_name",
            "contact.last_name",
            "email.email",
            "address.street_address",
            "address.city",
            "address.postal_code",
            "state.abbreviation",
            "country.iso_code",
        ])
        email_clause = "civicrm_email email ON contact.id = email.contact_id"
        if self.require_email:
            email_clause += " AND email.email IS NOT NULL"
        query.tables = [
            "civicrm_contact contact",
            email_clause,
            "civicrm_address address ON contact.id = address.contact_id",
            "civicrm_country country ON address.country_id = country.id",
            "civicrm_state_province state ON address.state_province_id = state.id",
        ]
        query.group_by = [
            "contact.id",
        ]
        query.order_by = [
            "contact.id",
        ]
        return query

class PagedGroup(ContactCache):
    pagesize = config.contact_cache_size

    def __init__(self, **kw):
        super(PagedGroup, self).__init__(**kw)
        self.offset = 0

    def buildQuery(self):
        query = super(PagedGroup, self).buildQuery()
        log.info("Limiting batch contact retrieval to {num} records.".format(num=self.pagesize))
        query.limit = self.pagesize
        query.offset = self.offset
        return query

    def next(self):
        query.offset += self.pagesize
        self.fetch()

class TaggedGroup(PagedGroup):
    """Select contacts based on included and excluded tags."""

    def __init__(self, tag, excludetag=None, **kw):
        super(TaggedGroup, self).__init__(**kw)
        self.tag = tag
        self.excludetag = excludetag

    def buildQuery(self):
        query = super(TaggedGroup, self).buildQuery()
        query.tables.extend([
            "civicrm_entity_tag entity_tag ON entity_tag.entity_id = contact.id AND entity_tag.tag_id = %(tag_id)s AND entity_tag.entity_table = 'civicrm_contact'",
        ])
        query.params.update({
            'tag_id': self.tag.id
        })

        if self.excludetag:
            query.tables.extend([
                "civicrm_entity_tag entity_tag_not ON entity_tag_not.entity_id = contact.id AND entity_tag_not.tag_id = %(excludetag_id)s AND entity_tag_not.entity_table = 'civicrm_contact'",
            ])
            query.where.extend([
                "entity_tag_not.id IS NULL"
            ])
            query.params.update({
                'excludetag_id': self.excludetag.id
            })

        return query
