import json
from bs4 import BeautifulSoup
import wtables.schema.DataType as dt


class Article(object):
    def __init__(self, articleId, title, tables):
        self.articleId = articleId
        self.title = title
        self.tables = tables

    def reprJSON(self):
        return dict(articleId=self.articleId, title=self.title, tables=self.tables)


class Table(object):
    def __init__(self, attrs, title, htmlMatrix, colHeaders, rowHeaders, startRows, nrows, ncols):
        self.tableId = ""
        self.attrs = attrs
        self.title = title
        self.htmlMatrix = htmlMatrix
        self.colHeaders = colHeaders
        self.rowHeaders = rowHeaders
        self.startRows = startRows
        self.nrows = nrows
        self.ncols = ncols
        self.innerTable = False

    def setInnerTable(self, innerTable):
        self.innerTable = innerTable

    def setTableId(self, tableId):
        self.tableId = tableId

    def getCell(self, row, col):
        return self.htmlMatrix[row][col]

    def getCellValue(self, row, col):
        soup = BeautifulSoup(self.htmlMatrix[row][col], "html.parser")
        return soup.get_text().replace("\n", " ").replace("\t", " ").strip()


    def getCellType(self, row, col):
        cell = BeautifulSoup(self.getCell(row, col), "html.parser")
        return dt.getDataType(cell)

    def getAttr(self, attr):
        return self.attrs.get(attr)

    def getHTMLMatrix(self):
        return self.htmlMatrix

    def toHTML(self):
        html = "<table>"
        for i in range(len(self.htmlMatrix)):
            html += "<tr>"
            for j in range(len(self.htmlMatrix[0])):
                html += self.htmlMatrix[i][j]
            html += "</tr>"
        html += "</table>"
        return html

    def reprJSON(self):
        return dict(tableId=self.tableId, title=self.title, htmlMatrix=self.htmlMatrix, startRows=self.startRows,
                    colHeaders=self.colHeaders, rowHeaders=self.rowHeaders, nrows=self.nrows, ncols=self.ncols)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'reprJSON'):
            return obj.reprJSON()
        else:
            return json.JSONEncoder.default(self, obj)


class ComplexDecoder(object):
    def default(self, obj):
        if 'articleId' in obj:
            tables = obj["tables"]
            listt2d = []
            for table in tables:
                table2d = Table(title=table["title"], htmlMatrix=table["htmlMatrix"], attrs=None,
                                startRows=table["startRows"], colHeaders=table["colHeaders"],
                                rowHeaders=table["rowHeaders"], nrows=table["nrows"], ncols=table["ncols"])
                table2d.setTableId(table["tableId"])
                listt2d.append(table2d)
            return Article(obj['articleId'], obj['title'], listt2d[:])
