import json
from bs4 import BeautifulSoup
import copy
import numpy as np
class Article(object):
    def __init__(self,articleId,title,tables):
        self.articleId=articleId
        self.title=title
        self.tables=tables
    def reprJSON(self):
        return dict(articleId=self.articleId,title=self.title, tables=self.tables)


class Table(object):
    def __init__(self,tableId, title, cells,attrs, html):
        self.tableId=tableId
        self.title=title
        self.cells=cells
        self.attrs = attrs
        self.html=html

    def getHeadersText(self):
        headers=[]
        if len(self.cells)>0:
            for i in range(len(self.cells)):
                for j in range(len(self.cells[i])):
                    c=self.cells[i][j]
                    if c!=None and c.type=="th":
                        soup=BeautifulSoup("<th>"+c.content+"</th>","html.parser")
                        if soup.get_text()!=None:
                            headers.append(soup.get_text().strip())
        return headers

    def getAttr(self, attr):
        attrDict=dict(self.attrs)
        return attrDict.get(attr)

    def getCell(self, row, col):
        return self.cells[row][col]

    def getHTMLMatrix(self):
        html = "<table>"
        for i in range(len(self.cells)):
            html += "<tr>"
            for j in range(len(self.cells[0])):
                if self.cells[i][j] ==None:
                    html += "<td></td>"
                    continue
                type= self.cells[i][j].type
                html +="<"+type+">"
                html += self.cells[i][j].content
                html += "</"+type+">"
            html += "</tr>"
        html+="</table>"
        return html


    def reprJSON(self):
        return dict(tableId=self.tableId,title=self.title, html=self.html)



class TableCell(object):
    def __init__(self, type, content,attrs):
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

class ComplexDecoder():
    def default(self, obj):
        if 'articleId' in obj :
            tables=obj['tables']
            listt2d=[]
            for t in tables:
                tablecells=np.array([[None]*len(t['cells'][0])]*len(t['cells']))
                for i in range(len(t['cells'])):
                    for j in range(len(t['cells'][i])):
                        cell=t['cells'][i][j]
                        if(cell!=None):
                            tablecells[i][j]=TableCell(cell['type'], cell['content'], cell['attrs'])
                listt2d.append(Table(t['tableId'], t['title'], tablecells[:], t['attrs']))
            return Article(obj['articleId'], obj['title'], listt2d[:])
