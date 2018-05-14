
"""Read HTML tags from soup object. Col/Row-span split was taken from next code:"""
#https://stackoverflow.com/questions/48393253/how-to-parse-table-with-rowspan-and-colspan
from bs4 import BeautifulSoup
from itertools import product

from source.wtables.preprocessing.JsonArticle import Table


def tableTo2d(table_tag):
    """Read HTML table from a soup tag, split col/row-span and generate a clean HTML table"""
    """TODO: Keep link values from cells"""

    rowspans = []  # track pending rowspans
    rows = table_tag.findChildren(['tr'])

    # first scan, see how many columns we need
    colcount = 0
    start=0
    tableName=""
    for r, row in enumerate(rows):
        cells = row.find_all(['td', 'th'], recursive=False)
        # count columns (including spanned).
        # add active rowspans from preceding rows
        # we *ignore* the colspan value on the last cell, to prevent
        # creating 'phantom' columns with no actual cells, only extended
        # colspans. This is achieved by hardcoding the last cell width as 1.
        # a colspan of 0 means “fill until the end” but can really only apply
        # to the last cell; ignore it elsewhere.
        colcount = max(
                colcount,
                sum(int(c.get('colspan', 1)) or 1 for c in cells[:-1]) + len(cells[-1:]) + len(rowspans))
        # update rowspan bookkeeping; 0 is a span to the bottom.
        rowspans += [int(c.get('rowspan', 1)) or len(rows) - r for c in cells]
        rowspans = [s - 1 for s in rowspans if s > 1]
        if start==0 and colcount==1:
            tableName=cells[0].get_text()
        start = 1

    # it doesn't matter if there are still rowspan numbers 'active'; no extra
    # rows to show in the table means the larger than 1 rowspan numbers in the
    # last table row are ignored.

    # build an empty matrix for all possible cells
    table = [[None] * colcount for row in rows]
    # fill matrix from row data
    rowspans = {}  # track pending rowspans, column number mapping to count
    start=0
    for row, row_elem in enumerate(rows):
        if start==0 and tableName!="":
            start=1
            continue
        #newTable+="<tr>"
        span_offset = 0  # how many columns are skipped due to row and colspans
        for col, cell in enumerate(row_elem.find_all(['td', 'th'], recursive=False)):
            # adjust for preceding row and colspans
            col += span_offset
            while rowspans.get(col, 0):
                span_offset += 1
                col += 1

            # fill table data
            rowspan = rowspans[col] = int(cell.get('rowspan', 1)) or len(rows) - row
            colspan = int(cell.get('colspan', 1)) or colcount - col
            # next column is offset by the colspan
            span_offset += colspan - 1
            value = cell.get_text()
            if value!=None:
                value=value.replace("\n"," ")
            for drow, dcol in product(range(rowspan), range(colspan)):
                try:
                    table[row + drow][col + dcol] = "<" + cell.name + ">" + value + "</" + cell.name + ">"
                    #newTable +=   # table[row + drow][col + dcol] = value
                except IndexError:
                    # rowspan or colspan outside the confines of the table
                    pass

        # update rowspan bookkeeping
        rowspans = {c: s - 1 for c, s in rowspans.items() if s > 1}
        #newTable += "</tr>"
    #newTable+="</table>"
    newTable="<table>"
    for r in range(len(table)):
        newTable += "<tr>"
        for c in range(len(table[r])):
            newTable += table[r][c]
        newTable += "</tr>"
    newTable+="</table>"
    tableObject=Table(tableName,newTable)
    return tableObject

def readTitle(tagSoup):
    return tagSoup.find("h1", {"class": "firstHeading"}).get_text()

def readTables(tagSoup):
    """Skip no Wikitables"""
    tables=tagSoup.findAll("table")
    wikitables=[]
    for t in tables:
        attrs=t.attrs.get("class")
        if (attrs!=None):
            if 'wikitable' in attrs:
                wikitables.append(t)
    return wikitables

