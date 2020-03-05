import os
import re
import sys
from wtables.utils import Pipey
from wtables.utils.ParseLink import *
from wtables.utils.ResultCombiner import *
import traceback
from wtables.schema.Article import ComplexDecoderTable
import json
def extractTableFile(tablefile):
    try:
        #print(os.path.join(FOLDER_JSON_FILES,file.split(".json")[0]+"_1.json"))
        file = open(os.path.join(FOLDER_JSON_FILES,tablefile), "r")
        obj = file.read()
        obj = json.loads(obj)
        table= ComplexDecoderTable().default(obj)
        return table
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        return None

def setUpText(text):
    textc=re.match(r'\d{4}$',text)
    if textc is not None:
        if int(text)>=1500 and int(text)<=2100:
            return 'yearheader'
    else:
        textc = re.match(r'(\d+)$', text)
        if textc is not None:
            return 'numberheader'
    return text

def removeTextAdds(text):
    textc = text.split("@")[0]
    if "__" in textc:
        textc = textc.split("__")[1]
    textc = " ".join(textc.split("**"))
    return textc

def cleanText(text):
    _text = re.sub(r'[^\w\s]', ' ', text)
    _text = _text.replace("\n"," ")
    _text=' '.join(e for e in _text.split(" ") if re.match(r'(\d+)$', e) is None)
    return _text

def cleanColHeaders(colHeaders):
    _colHeaders = [removeTextAdds(h) for h in colHeaders if "spancol" not in h]
    textHeaders = ""
    for h in _colHeaders:
        words = h.split("_")
        hclean = ""
        for w in words:
            hclean += setUpText(w) + " "
        textHeaders += hclean
    return cleanText(textHeaders)

def extractFeaturesFromTable(tableId):
    table=extractTableFile(tableId)
    if table is None:
        return ""
    vectorFeatures=[]
    colHeaders=table.colHeaders
    articleTitle = cleanText(table.articleTitle)
    tableTitle = cleanText(table.title)

    if len(colHeaders)==0:
        return ""
    else:
        textHeaders=cleanColHeaders(colHeaders)
        sentence=articleTitle.strip()+" "+tableTitle.strip()+" "+textHeaders.strip()
        sentence=sentence.split(" ")
        noRepeatedWords=[]
        for s in sentence:
            if s not in noRepeatedWords:
                if s!="":
                    noRepeatedWords.append(s)
        return tableId+"\t"+str(noRepeatedWords)+"\n"

def readFolder(input=0):
    files=os.listdir(FOLDER_JSON_FILES)
    for file in files:
        print("File", file)
        yield file
    print("End files..")
    yield Pipey.STOP

def processTable(tableId):
    result2 = extractFeaturesFromTable(tableId)
    print(result2)
    yield result2


if __name__ == '__main__':

    args = sys.argv[1:]
    #params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJson"  # params.get("json_files")
    fileOutput= args[0]
    pipeline = Pipey.Pipeline()
    pipeline.add(readFolder)
    pipeline.add(processTable, 4)
    pipeline.add(ResultCombiner(fileOutput))
    pipeline.run(100)


"""articleTitle="2010-2011 dadafs ,dadrwerw 4fs."
s=re.sub(r'[^\w\s]','',articleTitle)
print(s)
print(re.split(r'(\d+)',"dadsa43dfsd454"))
print([' '.join(e for e in s.split(" ") if re.match(r'(\d+)$', e) is None)])
print(re.match(r'(\d+)$', "fsdfdsf"))
#print([' '.join(e for e in s.split(" ") if re.compile(r'\d$', e)==None)])
for e in s.split(" "):
    print(e, re.match(r'(\d+)$', e))
print(re.match(r'\d{4}$',"fsfs"))
print(cleanColHeaders(['__rew','__1999','__eee777','__+','__dsadas_557','__dasda_fre','__4242_(a)','__1999_(+)']))
"""