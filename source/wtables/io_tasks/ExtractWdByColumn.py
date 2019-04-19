from wtables.schema.Article import *
import os

from wtables.features_cluster.FeaturesCluster import FeaturesCluster
from wtables.features_cluster.RelationCell import RelationCell
from wtables.features_cluster.Resource import Resource
from wtables.features_cluster.FeaturesTableCell import TableCell
from wtables.features_cluster.FeaturesTriple import TripleFeatures
from wtables.preprocessing import ReadHTML as readHTML
from wtables.preprocessing.TextProcessing import TextProcessing
from wtables.schema.Article import *
from wtables.wikidata_db.ConfigProperties import ConfigProperties
from wtables.wikidata_db.WikidataDAO import *
import traceback
import random
import gc
from bs4 import Tag

def extractTableFile(tableId):
    #Read tables from main directory
    try:
        file = open(os.path.join(FOLDER_JSON_FILES,tableId.replace(".","_") + ".json"), "r")
        obj = file.read()
        obj = json.loads(obj)
        #Converting json to Table object
        table= ComplexDecoderTable().default(obj)
        return table
    except Exception as ex:
        print(ex)
        return None

def extractCellResources(content):
    #Extract links from cells and get Wikidata IDs for cell (content)
    bscell = BeautifulSoup(content, "html.parser")
    linksCell = readHTML.readTableCellLinks(bscell)

    if linksCell == None or len(linksCell)==0:
        return []

    resources = {}
    for i, link in enumerate(linksCell):
        _link = wikiLink(link)
        if _link != None and _link != "":
                wd= wikidataDAO.getWikidataID(_link)
                if wd!="" and wd!=None:
                    resource=Resource(_link)
                    resource.setId(wd)
                    resources[_link]=resource
                else:
                    resource = Resource(_link)
                    resources[_link]=resource
        else:
            resource = Resource("ex: "+_link)
            resources["ex: "+_link] = resource
    #print("List resources:", resources)
    resources=list(resources.values())
    return resources

def extractArticleResource(articleTitle):
    #Convert article of title to Link and get Wikidata ID
    _link = wikiLink(articleTitle)
    resource = Resource(_link)
    if _link != None and _link != "":
        wd= wikidataDAO.getWikidataID(_link)
        if wd!="" and wd!=None:
            resource.setId(wd)

    return resource

def emptyRow(cells):
    #Counter rows for feature numRows. Avoid to count empty rows.
    ncells=len(cells)
    emptyCells=0
    for cell in cells:
        if cell=='<td></td>':
            emptyCells+=1
    if emptyCells==ncells:
        return True
    else:
        return False

def extractTableResources(tableId):
    out = ""
    table=extractTableFile(tableId)
    if table.htmlMatrix==None:
        return {}
    matrix=np.array(table.htmlMatrix)
    colHeaders=table.colHeaders
    relations={}
    matrixCells=np.array([[None]*len(matrix[0])]*len(matrix))
    dictResourcesByCol={}

    resArticle=extractArticleResource(table.articleTitle)
    out+=tableId+"\t"+'protag_article'+"\t"+resArticle.url+"\t"+str(resArticle.id)+"\n"
    for col in range(len(matrix[0])):
        colName=colHeaders[col]
        dictResourcesByCol[colName] = set()
        for row in range(table.startRows,len(matrix)):
            if (matrix[row][col])==None:
                print("Cell None:", table.tableId, matrix[row][col], row, col)
                continue
            resources = extractCellResources(matrix[row][col])
            for res in resources:
                dictResourcesByCol[colName].add((res.url, res.id))

    for col, resources in dictResourcesByCol.items():
        for (url, id) in resources:
            out+=tableId+"\t"+col +"\t"+url+"\t"+ str(id)+"\n"
    return out



def process(input=0):
    # for each document that we want to process,

    #files = os.listdir(path)
    cluster =""
    listClusters=[]
    clustert=[]
    cont=0
    #with gzip.open(FILE_OUTPUT, "wt") as fout:
    with gzip.open(FILE_CLUSTER, "rt") as fi:
            for line in fi:
                if cont==0:
                    cont+=1
                    continue
                cont+=1
                _line=line.replace("\n","").split("\t")
                table=_line[1]
                yield table

    yield Pipey.STOP


def processTable(tableId):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", tableId)
    result = extractTableResources(tableId)
    yield result


if __name__ == '__main__':

    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJson"  # params.get("json_files")
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillData()
    wikidataDAO.fillDomainRange()
    textProcessing = TextProcessing()
    FILE_CLUSTER=args[0]
    FILE_OUTPUT=args[1]
    #fileClusterRelations=args[2]
    #process()
    pipeline = Pipey.Pipeline()
    pipeline.add(process)
    pipeline.add(processTable, 8)
    pipeline.add(ResultCombiner(FILE_OUTPUT))
    pipeline.run(100)

#https://es.slideshare.net/GaneshBorle/word-embedding-to-document-distances