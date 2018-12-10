import re
def validateTable(table):
    if table is None:
        return False

    if (len(table.htmlMatrix) == 0):
        return False

    rows, cols = len(table.htmlMatrix), len(table.htmlMatrix[0])
    if table.getAttr("class") is None:
        if (cols < 2 or rows < 2):
            return False
        else:
            return True

    tclass = str(table.getAttr("class"))
    tclass = re.sub('\W+', " ", tclass)
    if ((tclass.lower() == "infobox")
        or ("infobox" in tclass.lower())):
        return False
    if (tclass == "box"
        or "box" in tclass.lower()
        or "metadata" in tclass.lower()
        or "maptable" in tclass.lower()
        or "vcard" in tclass.lower()):
        return False

    if (cols < 2 or rows < 2):
        return False
    else:
        splitClass = tclass.split()
        for cl in splitClass:
            if cl.strip().startswith("toc"):
                return False
        else:
            return True

def validateInnerNavBox(tableHTML):
    strrow=""
    for i in range(len(tableHTML)):
        strrow=" ".join(tableHTML[i])
    if "navbox" in strrow:
        return False
    else:
        return True

def headerValid(headers):
    contSpanCols = 1
    startCol = headers[0]
    for h in range(1, len(headers)):
        h2 = headers[h]
        if h2 == startCol:
            contSpanCols += 1
    ratio = contSpanCols / len(headers)
    if ratio >= 0.8:
        return False