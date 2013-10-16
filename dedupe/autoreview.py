from process.globals import config

from civicrm.tag import Tag
from contact_cache import TaggedGroup
from dedupe.action import Action
from fuzzy_text_matching import FuzzyTextMatching

class Autoreview(object):
    IDENTICAL = 'I'
    SIMILAR = 'S'
    UNRELATED = 'U'

    REVIEW = Tag.get('Review')
    AUTOREVIEWED = Tag.get('Autoreviewed - Unique')

    REC_KEEP = Action.get('Autoreview - Recommend keep')
    REC_SPAM = Action.get('Autoreview - Recommend spamblock')
    REC_DUP = Action.get('Autoreview - Recommend is duplicate')
    REC_NEWER = Action.get('Autoreview - Recommend update contact')
    REC_CONFLICT = Action.get('Autoreview - Recommend conflict resolution')

    EMAIL_EDIT_THRESHOLD = 2
    NAME_EDIT_THRESHOLD = 2
    ADDRESS_NUMBERS_EDIT_THRESHOLD = 2

    actionLookup = {
        #'III': AUTOREVIEWED & REC_DUP
        'IIS': REC_DUP,
        'IIU': REC_NEWER,
        'ISI': REC_DUP,
        'ISS': REC_DUP,
        'ISU': REC_CONFLICT,
        'IUI': REC_NEWER,
        'IUS': REC_NEWER,
        'IUU': REC_CONFLICT,
        'SII': REC_DUP,
        'SIS': REC_DUP,
        'SIU': REC_CONFLICT,
        'SSI': REC_DUP,
        'SSS': REC_CONFLICT,
        'SSU': REC_CONFLICT,
        'SUI': REC_CONFLICT,
        'SUS': REC_CONFLICT,
        'SUU': AUTOREVIEWED, # & REC_KEEP
        #U**: AUTOREVIEWED & REC_KEEP
    }

    def __init__(self):
        self.contactCache = TaggedGroup(Autoreview.REVIEW)

    def reviewBatch(self):
        self.contactCache.fetch(config.autoreview_job_size)

        # for ALL contacts,
        #self.review(

    def review(self, contact):
        for other in self.contactCache:
            result = {}
            result['other'] = other
            result['name'] = Autoreview.compareNames(contact['name'], other['name'])
            result['email'] = Autoreview.compareEmails(contact['email'], other['email'])
            result['address'] = Autoreview.compareAddresses(contact['address'], other['address'])
            action = self.determineAction(result)
            #XXX

    @staticmethod
    def compareNames(a, b):
        if a == b:
            return Autoreview.IDENTICAL

        # TODO: initials

        if FuzzyTextMatching.levenshteinDistance(a, b) <= Autoreview.NAME_EDIT_THRESHOLD:
            return Autoreview.SIMILAR

        return Autoreview.UNRELATED

    @staticmethod
    def compareEmails(a, b):
        if a == b:
            return Autoreview.IDENTICAL

        if FuzzyTextMatching.levenshteinDistance(a, b) <= Autoreview.EMAIL_EDIT_THRESHOLD:
            return Autoreview.SIMILAR

        return Autoreview.UNRELATED

    @staticmethod
    def compareAddresses(a, b):
        a['street_numbers'] = FuzzyTextMatching.extractNumbers(a['street_address'])
        b['street_numbers'] = FuzzyTextMatching.extractNumbers(b['street_address'])

        identical_hits = 0
        components = [
            'street_numbers',
            'street_address',
            'city',
            'postal_code',
            'country',
            'state',
        ]
        for key in components:
            if a[key] == b[key]:
                identical_hits += 1

        if identical_hits == len(components):
            return Autoreview.IDENTICAL

        # same postal code or closer
        if identical_hits >= 4:
            return Autoreview.SIMILAR

        if FuzzyTextMatching.levenshteinDistance(a['street_numbers'], b['street_numbers']) <= Autoreview.ADDRESS_NUMBERS_EDIT_THRESHOLD:
            return Autoreview.SIMILAR

        #if identical_hits == 0:
        return Autoreview.UNRELATED

    def determineAction(self, results):
        queue = None
        tag = None

        concatKey = results['name'] + results['email'] + results['address']
        if results['name'] == Autoreview.IDENTICAL and results['email'] == Autoreview.IDENTICAL and results['address'] == Autoreview.IDENTICAL:
            queue = Autoreview.AUTOREVIEWED
            tag = Autoreview.REC_DUP
        elif results['name'] == 'U':
            queue = Autoreview.AUTOREVIEWED
            tag = Autoreview.REC_KEEP
        else:
            queue = Autoreview.REVIEW
            tag = Autoreview.actionLookup[concat]

        return {
            'queue': queue,
            'tag': tag,
        }
