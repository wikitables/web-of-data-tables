import json
from bs4 import BeautifulSoup
import wtables.schema.DataType as dt
from wtables.schema.TableType import TableType

class Article(object):
    def __init__(self, articleId, title, tables):
        self.articleId = articleId
        self.title = title
        self.tables = tables

    def setTables(self, tables):
        self.tables=tables

    def reprJSON(self):
        return dict(articleId=self.articleId, title=self.title, tables=self.tables)


class Table(object):
    def __init__(self, attrs, title, htmlMatrix, html, colHeaders, rowHeaders, startRows, nrows, ncols):
        self.tableId = ""
        self.attrs = attrs
        self.title = title
        self.htmlMatrix = htmlMatrix
        self.colHeaders = colHeaders
        self.rowHeaders = rowHeaders
        self.startRows = startRows
        self.nrows = nrows
        self.ncols = ncols
        self.html=html
        self.tableType=None
        self.tableCells=None
        self.articleId=None
        self.articleTitle=None
        self.articlePath=None
        self.articleClass=None
        self.articleEntity = None
        self.columnsClasses={}

    def setArticleId(self, articleId):
        self.articleId=articleId

    def setArticleTitle(self, articleTitle):
        self.articleTitle=articleTitle

    def setArticlePath(self, articlePath):
        self.articlePath=articlePath

    def setArticleEntity(self, articleEntity):
        self.articleEntity=articleEntity

    def setStartRows(self, startRows):
        self.startRows=startRows

    def setColHeaders(self, colHeaders):
        self.colHeaders=colHeaders

    def setTableCells(self, tableCells):
        self.tableCells=tableCells

    def setTableType(self, tableType):
        self.tableType=tableType

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
                html += cleanCell(self.htmlMatrix[i][j])
            html += "</tr>"
        html += "</table>"
        whitelist = ['a', 'abbr', 'img']
        soupTag = BeautifulSoup(html, "html.parser")
        innerTags = soupTag.find_all()
        for tag in innerTags:
            if tag.name not in whitelist:
                tag.attrs = {}
        return str(soupTag)

    def setArticleClass(self, articleClass):
        self.articleClass=articleClass

    def setHTMLMatrix(self, htmlMatrix):
        self.htmlMatrix=htmlMatrix

    def setColumnClasses(self, columnClasses):
        self.columnClasses=columnClasses

    def reprJSON(self):
        return dict(tableId=self.tableId, title=self.title, htmlMatrix=self.htmlMatrix, startRows=self.startRows,
                    colHeaders=self.colHeaders, rowHeaders=self.rowHeaders, nrows=self.nrows, ncols=self.ncols, html=self.html,
                    tableType=self.tableType, articleTitle=self.articleTitle, articleId=self.articleId,
                    attrs=self.attrs, articlePath=self.articlePath, articleEntity=self.articleEntity, articleClass=self.articleClass,
                    columnClasses=self.columnClasses)



def cleanCell(text):
    whitelist = ['a', 'abbr', 'img']
    soupTag=BeautifulSoup(text, "html.parser")
    innerTags = soupTag.find_all()
    for tag in innerTags:
        if tag.name not in whitelist:
            tag.attrs = {}
    return str(soupTag)


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
                table2d = Table(title=table["title"], htmlMatrix=table["htmlMatrix"], attrs=table.get('attrs'),
                                startRows=table["startRows"], colHeaders=table["colHeaders"],
                                rowHeaders=table["rowHeaders"], nrows=table["nrows"], ncols=table["ncols"], html=table.get('html'),
                                )
                table2d.setTableId(table["tableId"])

                table2d.setTableType(TableType(table.get("tableType")))
                table2d.setArticleId(obj.get("articleId"))
                table2d.setArticleTitle(obj.get("articleTitle"))
                table2d.setArticlePath(obj.get("articlePath"))

                listt2d.append(table2d)
            return Article(obj['articleId'], obj['title'], listt2d[:])

class ComplexDecoderTable(object):
    def default(self, obj):
        if 'tableId' in obj:
            table2d = Table(title=obj["title"], htmlMatrix=obj["htmlMatrix"], attrs=obj.get('attrs'),
                                startRows=obj["startRows"], colHeaders=obj["colHeaders"],
                                rowHeaders=obj["rowHeaders"], nrows=obj["nrows"], ncols=obj["ncols"], html=obj.get("html")
                                )

            table2d.setTableId(obj["tableId"])

            table2d.setTableType(TableType(obj.get("tableType")))
            table2d.setArticleId(obj.get("articleId"))
            table2d.setArticleTitle(obj.get("articleTitle"))
            table2d.setArticlePath(obj.get("articlePath"))

            if obj.get("articleClass") is not None:
                table2d.setArticleClass(obj.get("articleClass"))
            if obj.get("articleEntity") is not None:
                table2d.setArticleEntity(obj.get("articleEntity"))
            if obj.get("columnClasses") is not None:
                table2d.setColumnClasses(obj.get("columnClasses"))
            return table2d