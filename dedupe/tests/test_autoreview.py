import unittest
from dedupe.autoreview import Autoreview

class TestAutoreview(unittest.TestCase):
    def setUp(self):
        self.reviewer = Autoreview()

    def test_compareNames(self):
        self.assertEqual(Autoreview.IDENTICAL, Autoreview.compareNames('Bar Foo', 'Bar Foo'))
        self.assertEqual(Autoreview.SIMILAR, Autoreview.compareNames('Bra Foo', 'Bar Foo'))
        self.assertEqual(Autoreview.UNRELATED, Autoreview.compareNames('Arctostaphylos', 'Bar Foo'))

    def test_compareEmails(self):
        self.assertEqual(Autoreview.IDENTICAL, Autoreview.compareEmails('foo@bar', 'foo@bar'))
        self.assertEqual(Autoreview.SIMILAR, Autoreview.compareEmails('foo2@bar', 'foo@bar'))
        self.assertEqual(Autoreview.UNRELATED, Autoreview.compareEmails('elem@ant', 'foo@bar'))

    def test_compareAddresses(self):
		oldAddress = {
			'street_address': '1701 Flightless Bird',
			'postal_code': '112233 BFF',
			'city': 'Dent',
			'country': 'UK',
			'state': 'Eastside',
		}
		nearAddress = {
			'street_address': '1710 F. Bd.',
			'postal_code': '112233 BFF',
			'city': 'Dent',
			'country': 'UK',
			'state': 'Eastside',
		}
		otherAddress = {
			'street_address': '1 Uptown',
			'postal_code': '323232',
			'city': 'Dent',
			'country': 'UK',
			'state': 'Eastside',
		}
		self.assertEqual(Autoreview.IDENTICAL, Autoreview.compareAddresses(oldAddress, oldAddress))
		self.assertEqual(Autoreview.SIMILAR, Autoreview.compareAddresses(nearAddress, oldAddress))
		self.assertEqual(Autoreview.UNRELATED, Autoreview.compareAddresses(otherAddress, oldAddress))

if __name__ == '__main__':
	unittest.main()
