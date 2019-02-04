import re
from bs4 import BeautifulSoup
from wtables.schema.TableType import *
def isSmallTable(tableSoup):
    rows = tableSoup.findChildren(['tr'])
    if len(rows) < 2:
        return True
    cols = []
    for r, row in enumerate(rows):
        cells = row.find_all(['td', 'th'], recursive=False)
        cols.append(len(cells))
        if max(cols) >= 2:
            break
    if max(cols) < 2:
        return True
    else:
        return False

def validateHTMLTable(tableSoup):
    parents = [p.name for p in tableSoup.findParents()]
    childrens = tableSoup.findChildren(['table'], recursive=True)

    if tableSoup.parent != None and ("th" in parents or "td" in parents or "tr"   in parents):
        return TableType.INNER_TABLE
    tclass=tableSoup.attrs.get("class")
    if tclass==None:
        if validateInnerCells(tableSoup) == False:
            return TableType.FORMAT_BOX
        if len(childrens)>=1:
            return TableType.WITH_INNER_TABLE
        if isSmallTable(tableSoup):
            return TableType.SMALLTABLE
        return TableType.NO_CSS_CLASS
    tclass = re.sub('\W+', " ", " ".join(tclass))
    if ((tclass.lower() == "infobox")
        or ("infobox" in tclass.lower())):
        return TableType.INFOBOX
    if (tclass == "box"
        or "box" in tclass.lower()
        or "metadata" in tclass.lower()
        or "maptable" in tclass.lower()
        or "vcard" in tclass.lower()
        or "navbox" in tclass.lower()):
        return TableType.FORMAT_BOX

    if validateInnerCells(tableSoup) == False:
        return TableType.FORMAT_BOX

    splitClass = tclass.split()
    for cl in splitClass:
        if cl.strip().lower()=="toc":
            return TableType.TOC

    if len(childrens) >= 1:
        return TableType.WITH_INNER_TABLE

    if isSmallTable(tableSoup):
        return TableType.SMALLTABLE


    return TableType.WIKITABLE

def validateInnerCells(tableTag):
    rows = tableTag.findChildren(['tr'])
    for r, row in enumerate(rows):
        cells = row.find_all(['td', 'th'], recursive=False)
        for c in cells:
            classCell = c.get('class')
            if classCell != None:
                classCell = " ".join(classCell)
                if "navbox" in classCell or "navgroup" in classCell:
                    return False
    return True

if __name__ == '__main__':
    smallTable = """<body><table class='wikitable toc nav'><tr><td>a</td><td>b</td></tr></table></body>"""
    smallTable1 = """<body><table><tr><tbody></tbody></tr></table></body>"""
    noclassTable = """<body><table><tbody><tr><td>a</td></tr><tr><td>b</td><td>c</td></tr></tbody></table></body>"""
    noclassTable1="""<body><table><tbody><tr><th></th><th></th></tr><tr><td></td></tr></tbody></table></body>"""
    innerTables="""<body><table><tbody><tr><td><table><tbody><tr></tr></tbody></table></td></tr><tr><th></th><th></th></tr><tr><td></td></tr></tbody></table></body>"""
    it=BeautifulSoup(smallTable, "html.parser")
    it=it.find_all("table")
    print(len(it))
    print(it[0])
    #print(it[1])
    type = validateHTMLTable(it[0])
    print(type)
    assert type == TableType.FORMAT_BOX
    type=validateHTMLTable(BeautifulSoup(smallTable, "html.parser").find_all("table")[0])
    assert type == TableType.FORMAT_BOX
    type = validateHTMLTable(BeautifulSoup(smallTable1, "html.parser").find_all("table")[0])
    assert type == TableType.SMALLTABLE
    type = validateHTMLTable(BeautifulSoup(noclassTable, "html.parser").find_all("table")[0])
    assert type == TableType.NO_CSS_CLASS
    type = validateHTMLTable(BeautifulSoup(noclassTable1, "html.parser").find_all("table")[0])
    assert type == TableType.NO_CSS_CLASS
    type = validateHTMLTable(BeautifulSoup(innerTables, "html.parser").find_all("table")[0])
    assert type==TableType.WITH_INNER_TABLE

