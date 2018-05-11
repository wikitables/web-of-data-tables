import json

class Article(object):
    def __init__(self,title,tables):
        self.title=title
        self.tables=tables
    def reprJSON(self):
        return dict(title=self.title, tables=self.tables)


class Table(object):
    def __init__(self,name, content):
        self.name=name
        self.content=content
    def reprJSON(self):
        return dict(name=self.name, content=self.content)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj,'reprJSON'):
            return obj.reprJSON()
        else:
            return json.JSONEncoder.default(self, obj)