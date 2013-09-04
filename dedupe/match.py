import json

# TODO: elaborate

class Match(object):
    def __init__(self):
        self.address = None
        self.email = None
        self.name = None

    def json(self):
        return json.dumps({
            "address": self.address,
            "email": self.email,
            "name": self.name,
        })

class EmailMatch(Match):
    def __init__(self, matchDescription):
        self.email = matchDescription
        
    def json(self):
        return json.dumps({
            "email": self.email,
        })
