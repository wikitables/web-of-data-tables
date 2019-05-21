class Resource(object):

    def __init__(self, url):
        self.url=url
        self.id=None
        self.name=None

    def setId(self, id):
        self.id=id

    def setName(self, name):
        self.name=name

    def toString(self):
        return self.url+" :"+self.id
