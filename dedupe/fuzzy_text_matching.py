import re

from Levenshtein import distance

class FuzzyTextMatching(object):
    @staticmethod
    def levenshteinDistance(string_a, string_b):
        return distance(string_a, string_b)

    @staticmethod
    def extractNumbers(address):
        return re.sub(r'/[^0-9 ]/', '', address).strip();

    #overkill: static function stripTrivial($address) {
    # See https://www.usps.com/send/official-abbreviations.htm -> Street suffixes, and Secondary units
