import json

class Article(object):
    def __init__(self,title,tables):
        self.title=title
        self.tables=tables
    def reprJSON(self):
        return dict(title=self.title, tables=self.tables)


class Table(object):
    def __init__(self,title, cells,attrs):
        self.title=title
        self.cells=cells
        self.attrs = attrs

    def getHeaders(self):
        headers=[]
        if len(self.cells)>0:
            for i in range(len(self.cells)):
                for j in range(len(self.cells[0])):
                    c=self.cells[i][j]
                    if c.type=="th":
                        headers.append(c.content)
        return headers

    def getAttr(self, attr):
        attrDict=dict(self.attrs)
        return attrDict.get(attr)

    def getCell(self, row, col):
        return self.cells[row][col]

    def reprJSON(self):
        return dict(title=self.title, cells=self.cells,attrs=self.attrs)



class TableCell(object):
    def __init__(self,type, content,attrs):
        self.type=type
        self.content=content
        self.attrs = attrs

    def getAttr(self, attr):
        attrDict=dict(self.attrs)
        return attrDict.get(attr)

    def reprJSON(self):
        return dict(type=self.type, content=self.content,attrs=self.attrs)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj,'reprJSON'):
                return obj.reprJSON()
        else:
            return json.JSONEncoder.default(self, obj)
