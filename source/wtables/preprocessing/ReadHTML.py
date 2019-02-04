# -*- coding: utf-8 -*-

"""Tables to 2D from soup object. Col/Row-span split was taken from next code:"""
# https://stackoverflow.com/questions/48393253
from bs4 import BeautifulSoup
from itertools import product
from wtables.schema.Article import Table
import re
import traceback

import wtables.schema.DataType as dt
from wtables.schema.TableType import TableType
import numpy as np

def getColumnType(ncol, startRow, htmlMatrix):
    """ Type with the most number of rows will be selected as column type:
        :param ncol: number of column to scan
        :param startRow: number of row from htmlMatrix, where table content starts.
        :param htmlMatrix: 2D array with html cell content.
        :return string value of column type.
    """
    dataTypes = {}
    for i in range(startRow, len(htmlMatrix)):
        soupText = BeautifulSoup(htmlMatrix[i][ncol], "html.parser")
        data = " ".join([s for s in soupText.strings if s.strip('\n ') != ''])
        data=re.sub('\\s+', ' ', data)
        type = dt.getDataType(data)
        if type == None:
            continue
        if dataTypes.get(type) != None:
            dataTypes[type] += 1
        else:
            dataTypes[type] = 1
    if len(dataTypes) == 0:
        return dt.DataType.other.value
    sortedDt = sorted(dataTypes.items(), key=lambda x: x[1])
    return sortedDt[len(sortedDt) - 1][0]


def remove_all_attrs_except(soup):
    whitelist = ['th', 'td', 'a', 'abbr']
    for tag in soup.find_all(True):
        if tag.name not in whitelist:
            tag.attrs = {}
    return soup


def tableTo2d(table_tag):
    try:
        tableAttr = table_tag.attrs
        table_tag = remove_all_attrs_except(table_tag)
        childrens = table_tag.findChildren(['table'], recursive=True)
        if len(childrens) >= 1:
            flag=True
            while (len(childrens)<10 and flag):
                newChildren = []
                for ch in childrens:
                    ch1 = ch.findChildren(['table'], recursive=True)
                    if ch1 != None and len(ch1) > 0:
                        newChildren.extend(ch1)
                    else:
                        newChildren.append(ch)
                    if (len(newChildren))>10:
                        flag=False
                        break
                nc = ''.join([str(c) for c in newChildren])
                if (nc == ''.join([str(c) for c in childrens])):
                    break
                else:
                    childrens = newChildren[:]
            if len(childrens)>10 or flag==False:
                table2d = saveIllTable(table_tag, TableType.WITH_INNER_TABLE.value)
                if table2d != None:
                    return [table2d]
                else:
                    return None
            listTables = parseInnerTables(table_tag, childrens)
            if listTables == None:
                formatTable = False
                rows = table_tag.findChildren(['tr'])
                for r, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'], recursive=False)
                    for c in cells:
                        classCell = c.get('class')
                        if classCell != None:
                            classCell = " ".join(classCell)
                            if "navbox" in classCell or "navgroup" in classCell:
                                formatTable = True
                                break
                    if formatTable:
                        break
                if formatTable:
                    table2d = saveIllTable(table_tag, TableType.FORMAT_BOX.value)
                else:
                    table2d = saveIllTable(table_tag, TableType.WITH_INNER_TABLE.value)
                if table2d != None:
                    return [table2d]
                return None
        else:
            listTables = [table_tag]
        resultTables = []
        for tablei in listTables:
            tableType = ""
            try:
                rowspans = []  # track pending rowspans
                rows = tablei.findChildren(['tr'])
                # first scan, see how many columns we need
                colcount = 0
                start = 0
                tableName = ""
                tableNumValues = 0
                errorTable = False
                for r, row in enumerate(rows):

                    cells = row.find_all(['td', 'th'], recursive=False)
                    # count columns (including spanned).
                    # add active rowspans from preceding rows
                    # we *ignore* the colspan value on the last cell, to prevent
                    # creating 'phantom' columns with no actual cells, only extended
                    # colspans. This is achieved by hardcoding the last cell width as 1.
                    # a colspan of 0 means “fill until the end” but can really only apply
                    # to the last cell; ignore it elsewhere.

                    sumCols = 0
                    for c in cells[:-1]:
                        classCell = c.get('class')
                        if classCell != None:
                            classCell = " ".join(classCell)
                            if "navbox" in classCell or "navgroup" in classCell:
                                tableType = TableType.FORMAT_BOX.value
                                raise Exception("Table format")

                        if c.get('colspan', 1) != "":
                            cspan = str(c.get('colspan', 1))
                            if int(cspan) >= 100:
                                tableType = TableType.ILL_FORMED.value
                                raise Exception("Colspan too large")
                            sumCols += int(cspan) or 1

                    colcount = max(
                        colcount,
                        sumCols + len(cells[-1:]) + len(rowspans))
                    # update rowspan bookkeeping; 0 is a span to the bottom.
                    arspan = []
                    for cl in cells:
                        rspan = str(cl.get('rowspan', 1))
                        if rspan == "":
                            arspan.append(1)
                        else:
                            arspan.append(int(rspan) or len(rows) - r)

                    rowspans += arspan

                    # rowspans += [(int(c.get('rowspan', 1)) or len(rows) - r for c in cells]

                    rowspans = [s - 1 for s in rowspans if s > 1]
                    if start == 0 and colcount == 1:
                        tableName = cells[0].get_text()
                    start = 1

                # it doesn't matter if there are still rowspan numbers 'active'; no extra
                # rows to show in the table means the larger than 1 rowspan numbers in the
                # last table row are ignored.
                if errorTable:
                    continue
                if tableName != "":
                    rows.pop(0)
                else:
                    caption = table_tag.find("caption")
                    if caption != None and len(caption) > 0:
                        tableName = caption.text
                if colcount == 0:
                    tableType = TableType.ILL_FORMED.value
                    raise Exception("Columns not found")

                # build an empty matrix for all possible cells
                tableHtml = [["<td></td>"] * colcount for row in rows]
                # fill matrix from row data
                rowspans = {}  # track pending rowspans, column number mapping to count

                for row, row_elem in enumerate(rows):
                    # newTable+="<tr>"
                    span_offset = 0  # how many columns are skipped due to row and colspans
                    for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)):
                        # adjust for preceding row and colspans
                        col += span_offset
                        while rowspans.get(col, 0):
                            span_offset += 1
                            col += 1

                        # fill table data
                        # rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row

                        rspan = str(cell.get('rowspan', 1))
                        if rspan == "":
                            rspan = 1
                        else:
                            rspan = int(rspan)

                        rowspan = rowspans[col] = rspan or len(rows) - row
                        cspan = str(cell.get('colspan', 1))

                        if cspan != "":

                            if int(cspan) >= 100:
                                tableType = TableType.ILL_FORMED.value
                                raise Exception("Colspan too large")
                            colspan = int(cspan) or colcount - col

                        else:
                            colspan = 1 or colcount - col
                        # next column is offset by the colspan
                        span_offset += colspan - 1
                        # value = cell.get_text()
                        # if value!=None:
                        #    value=value.replace("\n"," ")

                        if (cell.name == "td"):
                            tableNumValues += 1
                        for drow, dcol in product(range(rowspan), range(colspan)):
                            try:
                                del cell["id"]
                            except:
                                pass
                            try:
                                tableHtml[row + drow][col + dcol] = str(cell)
                            except IndexError:
                                # rowspan or colspan outside the confines of the table
                                pass

                    # update rowspan bookkeeping
                    rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}

                tableLen = len(tableHtml)

                if tableLen >= 2:
                    lastRow = tableHtml[tableLen - 1]
                    lastRow = set(lastRow)
                    if len(lastRow) == 1 and len(tableHtml[0]) > 1:
                        tableHtml.pop()
                startRows = 0
                nrows = len(tableHtml)
                ncols = 0
                colHeaders = []
                rowHeaders = []
                # if len(tableHtml) > 0:
                #
                #     #startRows, colHeaders = getMainColHeaders(tableHtml)
                #     #startRows += 1
                #     startRows, colHeaders = getColHeaderAllLevels(tableHtml)
                #     if len(set(colHeaders)) == 1 and colHeaders[0] == "":
                #         colHeaders = []
                #         startRows=0
                           #[h.lower().strip().replace(" ", "_") + "@" + str(
                            #getColumnType(i, startRows, tableHtml)) if h != "" else "spancol@" + str(
                            #getColumnType(i, startRows, tableHtml)) for i, h in enumerate(colHeaders)]

                #     rowHeaders = getRowHeaders(startRows, 0, tableHtml)
                #     nrows = tableLen - startRows
                #     ncols = len(tableHtml[0])
                # else:
                #     colHeaders = []
                #     rowHeaders = []
                if len(tableHtml) < 2 or len(tableHtml[0]) < 2:
                    tableType = TableType.SMALLTABLE.value
                else:
                    tableType = TableType.WELL_FORMED.value
                tableObject = Table(title=tableName, attrs=tableAttr, htmlMatrix=tableHtml, colHeaders=colHeaders,
                                    rowHeaders=rowHeaders, startRows=startRows, nrows=nrows, ncols=ncols, html="")
                tableObject.setTableType(tableType)
                # Return clean matrix html (newTable) and Table as object with attr.
                resultTables.append(tableObject)
            except:
                traceback.print_exc()
                if tableType == "":
                    newTable = saveIllTable(tablei, TableType.ILL_FORMED.value)
                else:
                    newTable = saveIllTable(tablei, tableType)
                if newTable != None:
                    resultTables.append(newTable)
                print("Catch error...")

        return resultTables  # newHtmlTable, tableObject
    except:
        traceback.print_exc()
        return None


def saveIllTable(table_tag, tableType):
    """
    If table could not be transformed to 2D matrix, it may be saved with its entire html code.
    :param table_tag: soup table
    :param tableType: table type
    :return: Table Object created
    """
    rows = table_tag.findChildren(['tr'])
    if len(rows) == 0:
        return None
    tableName = ""
    caption = rows[0].findChildren(['th', 'td'])
    if caption != None and len(caption) == 1:
        tableName = caption[0].text
    colHeaders = []
    rowHeaders = []
    nrows = len(rows)
    ncols = 0
    tableObject = Table(title=tableName, attrs={}, htmlMatrix=None, colHeaders=colHeaders,
                        rowHeaders=rowHeaders, startRows=0, nrows=nrows, ncols=ncols, html=str(table_tag))
    tableObject.setTableType(tableType)
    return tableObject


def getRowHeaders(start, colNumber, htmlMatrix):
    """
    Extracts a list of headers from colNumber.
    :param start: number of row where table content start.
    :param colNumber: number of column
    :param htmlMatrix: 2D array with html cell content.
    :return: list of row headers
    """
    headers = []
    if len(htmlMatrix) > 0:
        for j in range(start, len(htmlMatrix)):
            c = htmlMatrix[j][colNumber]
            h = getHeaderText(c)
            if h != None:
                headers.append(h)
    setrH = set(headers)
    if len(setrH) == 1 and headers[0] == "":
        headers = []
    else:
        headers = [h.lower().strip().replace(" ", "_") if h != "" else "spancol" for i, h in
                   enumerate(headers)]
    return headers


def parseInnerTables(tableParent, tableChildren):
    """ Return a list of tables. Only if every children have its own header.
        :param tableParent: soup table parent
        :param tableChildren: list of table soup children.
        :return listTables
    """
    rowsParent = tableParent.find_all(['tr'], recursive=False)
    if len(rowsParent) == 0:
        tbody = tableParent.find('tbody', recursive=False)
        if tbody != None:
            rowsParent = tbody.find_all(['tr'], recursive=False)
        else:
            return None
    header = []
    for row in rowsParent:
        for col, cell in enumerate(row.find_all(['th'], recursive=False)):
            cellText = cell.text
            if cellText != None and cellText != "":
                header.append(cell.text)
    if len(header) > 0 and (len(header) > len(tableChildren)):
        return None

    listHeaders = []
    listRowSizes = []
    legendTables = 0
    for t in tableChildren:
        ti_rows = t.findChildren(['tr'])
        listRowSizes.append(len(ti_rows))
        header = []
        tdCols = 0
        for i, row in enumerate(ti_rows):
            for col, cell in enumerate(row.find_all(['th'], recursive=False)):
                header.append(cell.text)
            for col, cell in enumerate(row.find_all(['td'], recursive=False)):
                if i == 0:
                    tdCols += 1
                b = cell.find("b")
                if b != None:
                    header.append(b.text)
        if len(header) >= 1:
            listHeaders.append(' '.join(header))
        if len(header) == 0 and tdCols == 1:
            legendTables += 1

    if len(listHeaders) + legendTables == len(tableChildren):
        return tableChildren[:]
    else:
        return None


def readTitle(tagSoup):
    title = tagSoup.find("h1", {"class": "firstHeading"})
    if title != None:
        return title.get_text()
    return tagSoup.title.string


def readTables(tagSoup):
    return tagSoup.findChildren("table")


def readTableCellLinks(cellSoup):
    """
    Extracts links from table cell. If it contains more than one, image links are skipped.
    :param cellSoup: soup content of table cell
    :return: list of links
    """
    listLinks = []
    links = cellSoup.find_all('a', href=True)
    if len(str(cellSoup)) <= 3000:
        lenlinks = len(links)
        for a in links:
            #parents_a = []
            #if lenlinks >= 2:
            #    parents_a = [p.name for p in a.findChildren()]
            #if "img" not in parents_a:
            listLinks.append(a["href"])
    return listLinks


def validateHeader(text):
    if len(text) <= 1000:
        return text


def getHeaderText(text):
    """
    Identifies if text contains a header (<th>, <br>, <abbr>)
    :param text: text to analize
    :return: text header cleaned.
    """

    text1 = re.sub('<br/*?\s*?>', " ", text)
    text1 = text1.replace("\n", " ")
    soup = BeautifulSoup(text1, "html.parser")
    th = soup.find("th")
    if th != None:
        thtext = th.text.replace("\n", " ").replace("\t", " ")
        thtext=re.sub('\\s+', ' ', thtext).strip()
        thtext = validateHeader(thtext)
        if thtext != None and thtext != "":
            if (thtext == thtext.upper()):
                b = soup.find("abbr")
                if b != None:
                    bt = b.get('title')
                    if bt != None:
                        return bt.strip()
                    else:
                        return ""
            return thtext
        else:
            b = soup.find("abbr")
            if b != None:
                bt = b.get('title')
                if bt != None:
                    return bt.strip()
                else:
                    return ""
            else:
                if thtext == "":
                    return ""
    else:

        b = soup.find("b")
        if b != None:
            btext = b.text.replace("\n", " ").replace("\t", " ").strip()
            btext = validateHeader(btext)
            if btext != None:
                return btext
        else:
            thtext=soup.text
            b = soup.find("abbr")
            if b != None:
                    bt = b.get('title')
                    if bt != None:
                        return bt.strip()
                    else:
                        return
    return None


def getColHeaderAllLevels(htmlMatrix, startRow,textProcessing):
    matrix = np.array(htmlMatrix)
    listOfLevelHeaders = []
    for i in range(startRow):
        listOfLevelHeaders.append(matrix[i])
    headersMatch = []
    for row in listOfLevelHeaders:
        cleanTagHeaders = []
        for col in range(len(row)):
            cell = BeautifulSoup(row[col], "html.parser")
            cell = cleanTableCellTag(cell)
            text = " ".join([s for s in cell.strings if s.strip('\n ') != ''])
            text = text.replace("*", "").replace("@", "")
            cleanTagHeaders.append(text)
            cleanTagHeaders = [textProcessing.cleanCellHeader(h) for h in cleanTagHeaders]
        headersMatch.append(cleanTagHeaders)
    lastRow = headersMatch[len(headersMatch) - 1]
    headersMatch[len(headersMatch) - 1] = ['spancol' if h == '' else h for h in lastRow]
    newHeader = []
    for col in range(len(headersMatch[0])):
        textCol = headersMatch[0][col]
        for row in range(1, len(headersMatch)):
            textCol += "**" + headersMatch[row][col]
        newHeader.append(textCol)
    newHeader = [re.sub('^\\**', '', h) for h in newHeader]
    if startRow > 1:
        newHeader = [h[:-2] if h.endswith("**") else h for h in newHeader]
    newHeader = textProcessing.orderHeaders(newHeader)
    newHeaderType = []
    for i, col in enumerate(newHeader):
        type = getColumnType(i, startRow, htmlMatrix)
        newHeaderType.append(newHeader[i] + "@" + str(type))
    return startRow, newHeaderType

def getTableCellText(htmlText):
    _tag=BeautifulSoup(htmlText,"html.parser")
    _tag=cleanTableCellTag(_tag)
    text=" ".join([s for s in _tag.strings if s.strip('\n ') != ''])
    text = re.sub('\\s+', ' ', text).strip()
    return text

def getTagTextNoLinks(tag):
    _tag=tag
    for a in _tag('a'):
        a.decompose()
    for span in _tag(['span', 'sup']):
        if span.attrs.get('class') != None:
            spanClass = span.attrs['class']
            if spanClass != None:
                spanClass = " ".join(spanClass)
                if 'sortkey' in spanClass or 'reference' in spanClass:
                    span.decompose()
    return _tag

def cleanTableCellTag(tag):
    _tag = tag
    for sup in _tag(['sup']):
        sup.decompose()
    for span in _tag(['span']):
        if span.attrs.get('class') != None:
            spanClass = span.attrs['class']
            if spanClass != None:
                spanClass = " ".join(spanClass)
                if 'sortkey' in spanClass :
                    span.decompose()
    return _tag

def getMainColHeaders(tableMatrix):
    """
    Extract column headers, goes through table rows and return the last row until a no header row is founded.
    :param tableMatrix: 2D table matrix with html cell content.
    :return: list of text headers.
    """
    rowNumber = 0
    colspanAcum = 0
    iterRow = 0
    numCol = len(tableMatrix[iterRow])
    headerFlag = False
    while (True):
        if iterRow >= len(tableMatrix):
            if headerFlag:
                return 1, getRowText(tableMatrix[0])
            else:
                return 0, []
        headers = getHeaders(tableMatrix[iterRow])
        seth = set(headers)
        if (len(seth) == 0) :
            if headerFlag:
                break
            rowText= getRowText(tableMatrix[iterRow])
            rowText=set([h for h in rowText if h!=""])
            if (len(rowText)>0):
                break;
            else:
                iterRow += 1
                continue
        if (len(seth) == 1 and headers[0] == ""):
            iterRow += 1
            continue
        else:
            headerFlag = True
            colspanAcum=0
            if len(headers) > 0:
                for j in range(numCol):
                    c = tableMatrix[iterRow][j]
                    colspan = getColspan(c)
                    colspanAcum += colspan
                if len(headers) < numCol :
                    rowText = getRowText(tableMatrix[iterRow])
                    rowText = set([h for h in rowText if h != ""])
                    if (len(rowText) > 0):
                        break;
                    else:
                        iterRow += 1
                        continue
                if colspanAcum == numCol and len(headers) == numCol:
                    return iterRow+1, headers
            else:
                break
            iterRow += 1
    if iterRow > 0 and headerFlag:
        headers = getRowText(tableMatrix[iterRow - 1])
        return iterRow, headers
    else:
        if headerFlag:
            h0 = getRowText(tableMatrix[0])
            if len(h0) > 0:
                return 1, h0
        else:
            return 0, []


def getHeaders(tableRow):
    headers = []
    for cell in tableRow:
        h = getHeaderText(cell)
        if h != None:
            headers.append(h)
    return headers


def getRowText(tableRow):
    headers = []
    for cell in tableRow:
        text=getTableCellText(cell)
        headers.append(text)
    return headers


def getColspan(text):
    soup = BeautifulSoup(text, "html.parser")
    h = soup.find("th")
    if h == None:
        h = soup.find("td")
    try:
        if h["colspan"] != None:
            return int(h["colspan"])
    except:
        return 1


def removeSpanAttrs(soup):
    for tag in soup.find_all(True):
        tag.attrs = {}
    return soup




if __name__ == '__main__':
    innerTables = """<table><tbody><tr><td><tbody><tr><table><tr><td>1</td><td>2</td></tr></table></tr></tbody></td></tr><tr><th>g</th></tr>
    <tr><td><table><tr><td>x1</td><td>x2</td></tr><tr><td>x10</td><td>x20</td></tr></table></td><td>fin</td></tr>
    <tr><td>a</td><td>b</td><td>c</td></tr></tbody></table>"""
    rows = BeautifulSoup(innerTables, "html.parser").find_all(["tr"])
    print(rows)
    table = """"""
    list2d = tableTo2d(BeautifulSoup(innerTables, "html.parser"))
    print(len(list2d))
    if list2d[0].htmlMatrix==None:
        print(list2d[0].html)
    else:
        print(list2d[0].toHTML())
