
"""Read HTML tags from soup object. Col/Row-span split was taken from next code:"""
#https://stackoverflow.com/questions/48393253/how-to-parse-table-with-rowspan-and-colspan
from bs4 import BeautifulSoup
from itertools import product
import re
from source.wtables.preprocessing.JsonArticle import Table, TableCell


def tableTo2d(table_tag):
    """Read HTML table from a soup tag, split col/row-span and generate a clean HTML table"""

    rowspans = []  # track pending rowspans
    rows = table_tag.findChildren(['tr'])

    # first scan, see how many columns we need
    colcount = 0
    start=0
    tableName=""
    tableNumValues=0
    tableAttr=table_tag.attrs
    for r, row in enumerate(rows):

        cells = row.find_all(['td', 'th'], recursive=False)
        # count columns (including spanned).
        # add active rowspans from preceding rows
        # we *ignore* the colspan value on the last cell, to prevent
        # creating 'phantom' columns with no actual cells, only extended
        # colspans. This is achieved by hardcoding the last cell width as 1.
        # a colspan of 0 means “fill until the end” but can really only apply
        # to the last cell; ignore it elsewhere.

        sumCols=0
        for c in cells[:-1]:
            if c.get('colspan', 1)!="":
                cspan=str(c.get('colspan', 1))
                try:
                    sumCols += int(cspan) or 1
                except(ValueError):
                    return None,None


        colcount = max(
                colcount,
                sumCols + len(cells[-1:]) + len(rowspans))
        # update rowspan bookkeeping; 0 is a span to the bottom.
        arspan=[]
        for cl in cells:
            rspan= str(cl.get('rowspan', 1))
            if rspan =="":
                arspan.append(1)
            else:
                try:
                    arspan.append(int(rspan) or len(rows) - r)
                except(ValueError):
                    return None,None

        rowspans +=arspan

        #rowspans += [(int(c.get('rowspan', 1)) or len(rows) - r for c in cells]

        rowspans = [s - 1 for s in rowspans if s > 1]
        if start==0 and colcount==1:
            tableName=cells[0].get_text()
        start = 1

    # it doesn't matter if there are still rowspan numbers 'active'; no extra
    # rows to show in the table means the larger than 1 rowspan numbers in the
    # last table row are ignored.

    if tableName != "":
        rows.pop(0)


    # build an empty matrix for all possible cells
    table = [[None] * colcount for row in rows]
    tableHtml = [[None] * colcount for row in rows]
    # fill matrix from row data
    rowspans = {}  # track pending rowspans, column number mapping to count

    for row, row_elem in enumerate(rows):
        #newTable+="<tr>"
        span_offset = 0  # how many columns are skipped due to row and colspans
        for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)):
            # adjust for preceding row and colspans
            col += span_offset
            while rowspans.get(col, 0):
                span_offset += 1
                col += 1

            # fill table data
            #rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row

            rspan = str(cell.get('rowspan', 1))
            if rspan == "":
                rspan = 1
            else:
                try:
                    rspan = int(rspan)
                except(ValueError):
                    return None,None


            rowspan = rowspans[col]= rspan or len(rows) - row
            cspan=str(cell.get('colspan', 1))

            if cspan!="":
                try:
                    colspan = int(cspan) or colcount - col
                except(ValueError):
                    return None,None
            else:
                colspan = 1 or colcount - col
            # next column is offset by the colspan
            span_offset += colspan - 1
            #value = cell.get_text()
            #if value!=None:
            #    value=value.replace("\n"," ")
            value=""
            for content in cell.contents:
                value+=str(content)
            if(cell.name=="td"):
                tableNumValues+=1
            for drow, dcol in product(range(rowspan), range(colspan)):
                try:
                    table[row + drow][col + dcol] = TableCell(type=cell.name, attrs=cell.attrs, content=value)#"<" + cell.name + ">" + value + "</" + cell.name + ">"
                    tableHtml[row + drow][col + dcol] = "<" + cell.name + ">" + value + "</" + cell.name + ">"
                    #newTable +=   # table[row + drow][col + dcol] = value
                except IndexError:
                    # rowspan or colspan outside the confines of the table
                    pass

        # update rowspan bookkeeping
        rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}
        #newTable += "</tr>"
    #newTable+="</table>"
    #HTML code
    newTable="<table>"
    for r in range(len(tableHtml)):
        newTable += "<tr>"
        for c in range(len(tableHtml[r])):
            if (tableHtml[r][c]!=None):
                newTable += tableHtml[r][c]
        newTable += "</tr>"
    newTable+="</table>"
    tableObject=Table(title=tableName, cells=table,attrs=tableAttr)
    #Return clean matrix html (newTable) and Table as object with attr.
    return newTable, tableObject

def readTitle(tagSoup):
    title= tagSoup.find("h1", {"class": "firstHeading"})
    if title!=None:
        return title.get_text()
    return tagSoup.title.string

def readTables(tagSoup):
    return tagSoup.findAll("table")

def readHeaders(tableSoup):
    rows = tableSoup.findChildren(['tr'])
    tableHeaders = []
    for r, row in enumerate(rows):
        headers = row.find_all(['th'], recursive=False)
        for h in headers:
            tableHeaders.append(h.get_text())
    return tableHeaders

