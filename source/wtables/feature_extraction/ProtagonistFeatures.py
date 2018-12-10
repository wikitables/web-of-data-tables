import re
import numpy as np
def fractionCellsWithUniqContent(table, indexCol):
    setValues=set()
    for i in range(table.startRows,table.nrows):
        #print('i: ', i)
        setValues.add(table.getCellValue(i,indexCol))
    divs=(table.nrows-table.startRows)
    if divs==0:
        return 0
    return len(setValues)/divs

def averageNumberWordsInEachCell(table, indexCol):
    dictrow={}
    for i in range(table.startRows,table.nrows):
        wordList = re.sub("[^\w]", " ", table.getCellValue(i,indexCol)).split()
        dictrow[i]=len(wordList)
    return np.average(list(dictrow.values()))

def extractProtagonistFeatures(table):
    colHeaders=table.colHeaders
    dictColFeatures={}
    for i in range(len(colHeaders)):
        dictColFeatures[colHeaders[i]]={'FRACT_CELL_UNIQ_CONTENT':fractionCellsWithUniqContent(table, i),
                                        'AVG_CELL_WORDS': averageNumberWordsInEachCell(table, i),
                                        'DATA_TYPE': colHeaders[i].split("@")[1],
                                        'INDEX_POS': i}
    return dictColFeatures

