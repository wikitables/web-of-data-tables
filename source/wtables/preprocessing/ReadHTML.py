"""Read HTML tags from soup object. Col/Row-span split was taken from next code:"""
# https://stackoverflow.com/questions/48393253
from bs4 import BeautifulSoup
from itertools import product

from docutils.nodes import row

from wtables.schema.Article import Table
import re
import traceback

import wtables.schema.DataType as dt


def getColumnType(ncol, startRow, htmlMatrix):
    dataTypes = {}
    for i in range(startRow, len(htmlMatrix)):
        soupText=BeautifulSoup(htmlMatrix[i][ncol], "html.parser")
        b = soupText.find_all("b")
        data = ""
        if b != None and len(b) > 0:
            for btext in b:
                data += btext.text + " "
        else:
            data = soupText.get_text()
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
    whitelist = ['th', 'td', 'a','abbr']
    for tag in soup.find_all(True):
        if tag.name not in whitelist:
            tag.attrs = {}
    return soup


def tableTo2d(table_tag):
    try:
        listTables = []

        # rows = table_tag.findChildren(['tr'])
        tableAttr = table_tag.attrs
        table_tag = remove_all_attrs_except(table_tag)
        # tableInside = table_tag.findChildren(['table'])
        parents = [p.name for p in table_tag.findParents()]
        childrens = table_tag.findChildren(['table'])
        if len(childrens) >= 1:
            listTables = parseInnerTables(table_tag, childrens)
            if listTables == None:
                return None
        else:
            if table_tag.parent != None and ("th" in parents or "td" in parents):
                tableObject = Table(title=None, attrs=None, htmlMatrix=None, colHeaders=None,
                                    rowHeaders=None, startRows=None, nrows=None, ncols=None)
                tableObject.setInnerTable(True)
                return [tableObject]
            else:
                listTables.append(table_tag)
        resultTables = []
        for tablei in listTables:
            rowspans = []  # track pending rowspans
            rows = tablei.findChildren(['tr'])
            # first scan, see how many columns we need
            colcount = 0
            start = 0
            tableName = ""
            tableNumValues = 0

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
                    if c.get('colspan', 1) != "":
                        cspan = str(c.get('colspan', 1))
                        try:
                            if int(cspan) >= 100:
                                return None
                            sumCols +=int(cspan) or 1
                        except(ValueError):
                            return None

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
                        try:
                            arspan.append(int(rspan) or len(rows) - r)
                        except(ValueError):
                            return None

                rowspans += arspan

                # rowspans += [(int(c.get('rowspan', 1)) or len(rows) - r for c in cells]

                rowspans = [s - 1 for s in rowspans if s > 1]
                if start == 0 and colcount == 1:
                    tableName = cells[0].get_text()
                start = 1

            # it doesn't matter if there are still rowspan numbers 'active'; no extra
            # rows to show in the table means the larger than 1 rowspan numbers in the
            # last table row are ignored.

            if tableName != "":
                rows.pop(0)
            else:
                caption = table_tag.find("caption")
                if caption != None and len(caption) > 0:
                    tableName = caption.text
            if colcount == 0:
                return  None
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
                        try:
                            rspan = int(rspan)
                        except(ValueError):
                            return None

                    rowspan = rowspans[col] = rspan or len(rows) - row
                    cspan = str(cell.get('colspan', 1))

                    if cspan != "":
                        try:
                            if int(cspan)>=100:
                                return None
                            colspan = int(cspan) or colcount - col
                        except(ValueError):
                            return None
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
                        except :
                            print("error id cell")
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
            startRows = 1
            nrows = 0
            ncols = 0
            if len(tableHtml) > 0:
                startRows, colHeaders = getMainColHeaders(tableHtml)
                startRows += 1
                if len(set(colHeaders)) == 1 and colHeaders[0] == "":
                    colHeaders = []
                else:
                    colHeaders = [h.lower().strip().replace(" ", "_") + "@" + str(
                        getColumnType(i, startRows, tableHtml)) if h != "" else "spancol@" + str(
                        getColumnType(i, startRows, tableHtml)) for i, h in enumerate(colHeaders)]

                rowHeaders = getRowHeaders(startRows, 0, tableHtml)
                nrows = tableLen - startRows
                ncols = len(tableHtml[0])
            else:
                colHeaders = []
                rowHeaders = []

            tableObject = Table(title=tableName, attrs=tableAttr, htmlMatrix=tableHtml, colHeaders=colHeaders,
                                rowHeaders=rowHeaders, startRows=startRows, nrows=nrows, ncols=ncols)

            # Return clean matrix html (newTable) and Table as object with attr.
            resultTables.append(tableObject)
        return resultTables  # newHtmlTable, tableObject
    except:
        return None

def getRowHeaders(start, colNumber, htmlMatrix):
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
    rowsParent = tableParent.find_all(['tr'], recursive=False)
    if len(rowsParent) == 0:
        tbody = tableParent.find('tbody', recursive=False)
        if tbody!=None:
            rowsParent = tbody.find_all(['tr'], recursive=False)
        else:
            return [tableParent]
    emptycells = 0
    allcells = 0
    row0 = []
    irow = 0
    ititle=0

    for row in rowsParent:
        for col, cell in enumerate(row.find_all(['th', 'td'], recursive=False)):
            allcells += 1
            cellText = ''.join(cell.findAll(text=True, recursive=False))
            cellText = cellText.replace(" ", "")
            cellText = cellText.replace("\n", "")
            cellText = cellText.replace("\t", "")
            row0.append(cellText)
            cellChild=cell.findChildren(['table'])
            if cellChild==None or len(cellChild)==0 :
                if cellText == None or cellText == "":
                    emptycells += 1
        if irow == 0:
            setr0 = set(row0)
            if "" in setr0:
                setr0.remove("")
            if len(setr0)>0:
                irow += 1
                if len(setr0) == 1:
                    ititle=1
        row0.clear()
    allcells = allcells - emptycells-ititle
    if allcells != len(tableChildren):
        return [tableParent]
    listHeaders = []
    reparable = False
    listRowSizes = []
    for t in tableChildren:
        ti_rows = t.findChildren(['tr'])
        listRowSizes.append(len(ti_rows))
        for row in ti_rows:
            header = []
            for col, cell in enumerate(row.find_all(['th'], recursive=False)):
                header.append(cell.text)

            seth = list(set(header))
            if len(seth) > 1:
                seth.sort()
                listHeaders.append(seth)
                break
    cont = 0
    for i in range(len(listHeaders) - 1):
        if set(listHeaders[i]) == set(listHeaders[i + 1]):
            cont += 1
    if cont == len(listHeaders) - 1:
        reparable = True
    if reparable:
        return tableChildren
    else:

        cont = 0
        for i in range(len(listRowSizes) - 1):
            if listRowSizes[i] == listRowSizes[i + 1]:
                cont += 1

        if cont == len(listRowSizes) - 1:
            alltables = tableParent.findChildren("table")
            listtrs = {}
            trs = alltables[0].findAll('tr')
            for i in range(len(trs)):
                listtrs[i] = [trs[i].find_all(['th', 'td'])]
            for i in range(1, len(alltables)):
                trs_i = alltables[i].findAll('tr')
                for j in range(len(trs)):
                    listtrs[j].append(trs_i[j].find_all(['th', 'td']))
            tablejoin = "<table>"
            for k, v in listtrs.items():
                tablejoin += "<tr>"
                tablejoin += ' '.join([str(item) for sublist in v for item in sublist])
                tablejoin += "</tr>"
            tablejoin += "</table>"
            tablejoin = BeautifulSoup(tablejoin, "html.parser")
            return [tablejoin]
        else:
            return tableChildren

def readTitle(tagSoup):
    title = tagSoup.find("h1", {"class": "firstHeading"})
    if title != None:
        return title.get_text()
    return tagSoup.title.string


def readTables(tagSoup):
    return tagSoup.findChildren("table")


def readTableCellLinks(cellSoup):
    listLinks = []
    links = cellSoup.find_all('a', href=True)
    if len(str(cellSoup)) <= 3000:
        lenlinks=len(links)
        for a in links:
            parents_a=[]
            if lenlinks>=2:
                parents_a=[p.name for p in a.findChildren()]
            if "img"  not in parents_a:
                listLinks.append(a["href"])
    return listLinks


def validateHeader(text):
    if len(text) <= 70:
        return text


def getHeaderText(text):
    text1 = text.replace("'s", "")
    text1 = text1.replace("-", " ")
    text1 = text1.replace("(s)", "")
    text1 = re.sub('<br/*?\s*?>', " ", text1)
    text1 = text1.replace("\n", " ")
    text1 = re.sub(' +', ' ', text1)

    soup = BeautifulSoup(text1, "html.parser")
    th = soup.find("th")
    if th != None:
        thtext = th.text.replace("\n", " ").replace("\t", " ")
        thtext = thtext.replace("%", "percentage")
        thtext = validateHeader(thtext)
        if thtext != None and thtext != "":
            return thtext
        else:
            b = soup.find("abbr")
            if b != None:
                bt= b['title']
                if bt!=None:
                    return bt.strip()
            else:
                if thtext=="":
                    return thtext
    else:
        b = soup.find("b")
        if b != None:
            btext = b.text.replace("\n", " ").replace("\t", " ").strip()
            btext = validateHeader(btext)
            if btext != None:
                return btext
        else:
            btext = soup.text
            if btext == "":
                return btext
    return None

def getTableCellText(tdText):

    text1 = re.sub('<br/*?\s*?>', " ", tdText)
    text1 = text1.replace("\n", " ")
    text1 = re.sub(' +', ' ', text1)

    soup = BeautifulSoup(text1, "html.parser")
    cell = soup.find("th")
    if cell==None:
        cell =soup.find("td")
    text1=cell.text.replace("\n", " ").replace("\t", " ").strip()
    return text1

def getMainColHeaders(tableMatrix):
    rowNumber = 0
    colspanAcum = 0
    iterRow = 0
    numCol = len(tableMatrix[iterRow])
    headerFlag=False
    while (True):
        if iterRow >= len(tableMatrix):
            return 0, getRowText(tableMatrix[0])
        headers = getHeaders(tableMatrix[iterRow])
        seth = set(headers)
        if ((len(seth) == 0) or (len(seth) == 1 and headers[0] == "")):
            if headerFlag:
                break
            else:
                iterRow += 1
        else:
            headerFlag=True
            if len(headers) > 0:
                for j in range(numCol):
                    c = tableMatrix[rowNumber][j]
                    colspan = getColspan(c)
                    colspanAcum += colspan
                if len(headers) < numCol:
                    break
                if colspanAcum==numCol and len(headers) == numCol:
                    return iterRow, headers
            else:
                break
            iterRow += 1
    if iterRow > 0 and headerFlag:
        headers = getRowText(tableMatrix[iterRow - 1])
        return iterRow - 1, headers
    else:
        if headerFlag:
            h0=getRowText(tableMatrix[0])
            if len(h0)>0:
                return 0, h0
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
        h = getHeaderText(cell)
        if h != None:
            headers.append(h)
        else:
            headers.append("")
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