import numpy as np
from wtables.schema.Article import *
from wtables.features_cluster.FeaturesTableCell import TableCell
from wtables.features_cluster.RelationCell import RelationCell
from wtables.features_cluster.Resource import Resource
from wtables.preprocessing import ReadHTML as readHTML
from wtables.wikidata_db.WikidataDAO import *
from wtables.wikidata_db.ConfigProperties import ConfigProperties
from wtables.preprocessing.TextProcessing import TextProcessing
import os

def extractTableFile(tableId):
    try:
        file = open(os.path.join(FOLDER_JSON_FILES,tableId.replace(".","_") + ".json"), "r")
        obj = file.read()
        obj = json.loads(obj)
        table= ComplexDecoderTable().default(obj)
        return table
    except Exception as ex:
        print(ex)
        return None

def extractCellResources(content):
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
                    resources[wd]=resource
    #print("List resources:", resources)
    resources=list(resources.values())
    return resources

def extractArticleResource(articleTitle):
    _link = wikiLink(articleTitle)
    if _link != None and _link != "":
        wd= wikidataDAO.getWikidataID(_link)
        if wd!="" and wd!=None:
            resource=Resource(_link)
            resource.setId(wd)
            return resource


def extractTableResources(table):
    if table.htmlMatrix==None:
        return {}
    matrix=np.array(table.htmlMatrix)
    colHeaders=table.colHeaders
    relations={table.tableId:{}}
    matrixCells=np.array([[None]*len(matrix[0])]*len(matrix))
    for row in range(table.startRows,len(matrix)):
        for col in range(len(matrix[0])):
            colName=colHeaders[col]
            resources = extractCellResources(matrix[row][col])
            if len(resources)>0:
                tableCell = TableCell(table.tableId, row, col, colName)
                tableCell.setResources(resources)
                format = formatFeatures(matrix[row][col])
                tableCell.setFormatFeatures(format)
                matrixCells[row][col]=tableCell
            else:
                matrixCells[row][col] = None


    resourceArticle = extractArticleResource(table.articleTitle)
    if resourceArticle is not None:
        for col2 in range(len(matrixCells[0])):
            colName1='protag_article@3'
            colName2 = colHeaders[col2]
            key=colName1+"##"+colName2
            for row in range(table.startRows, len(matrix)):
                cell1 = TableCell(table.tableId, row, -1, colName1)
                cell1.setFormatFeatures({})

                cell1.setResources([resourceArticle])
                cell2 = matrixCells[row][col2]
                if cell2==None:
                    continue
                res2 = cell2.resources
                if len(res2)>0:
                    relation = RelationCell(cell1, cell2)
                    if relations[table.tableId].get(key) == None:
                        relations[table.tableId][key] = [relation]
                    else:
                        relations[table.tableId][key].append(relation)

    for col1 in range(len(matrix[0])):
        colName1=colHeaders[col1]
        for col2 in range(col1+1, len(matrix[0])):
            colName2 = colHeaders[col2]
            key=colName1+"##"+colName2
            for row in range(table.startRows, len(matrix)):
                if matrix[row][col1]==matrix[row][col2]:
                    continue
                cell1=matrixCells[row][col1]
                cell2 = matrixCells[row][col2]
                if cell1==None or cell2==None:
                    continue
                res1=cell1.resources
                res2=cell2.resources
                if len(res1)>0 and len(res2)>0:
                    relation=RelationCell(cell1, cell2)
                    if relations[table.tableId].get(key)==None:
                        relations[table.tableId][key]=[relation]
                    else:
                        relations[table.tableId][key].append(relation)

    return relations

def extractRelations(tableRelations):
    allRelations=[]
    newTableRelations={}
    dictColRelations={}
    generalRelations={}
    for tableId, colRelations in tableRelations.items():
        for cols, relations in colRelations.items():
            for relation in relations:
                _relation=relation
                resources1=relation.tableCell1.resources
                resources2=relation.tableCell2.resources
                nameCol1=relation.tableCell1.colName
                nameCol2 = relation.tableCell2.colName
                allPredicates=[]
                for res1 in resources1:
                    for res2 in resources2:
                        if res1.id!=res2.id:
                            predicates= wikidataDAO.getRelations(res1.id, res2.id)
                            #print('relation: ', res1.id, res2.id, predicates)
                            allPredicates.extend(predicates)
                            if relation.predicates.get(res1.id)==None:
                                relation.predicates[res1.id]={res2.id:predicates}
                            else:
                                relation.predicates[res1.id][res2.id]= predicates
                if len(resources1)>0 and len(resources2)>0:
                    relationsByCols=generalRelations.get(cols)
                    if len(allPredicates) > 0:
                        if relationsByCols is None:
                            generalRelations[cols]=list(set(allPredicates))
                        else:
                            relationsByCols=set(relationsByCols)
                            generalRelations[cols]=list(relationsByCols.union(set(allPredicates)))
            dictColRelations[cols]=relations
        newTableRelations[tableId]=dictColRelations
    return generalRelations,newTableRelations

def getTriples(relation, generalRelations):
    res1 = relation.tableCell1.resources
    res2 = relation.tableCell2.resources
    #print('generalRelations',generalRelations)
    key=relation.tableCell1.colName+"##"+relation.tableCell2.colName
    pos=str(relation.tableCell1.row)+":"+str(relation.tableCell1.col)+":"+str(relation.tableCell2.col)
    triples=[]
    for r1 in res1:
        #print(r1)
        #print('predicates: ', relation.predicates)
        obj = relation.predicates.get(r1.id)
        if obj != None:
            for r2 in res2:
                pred = obj.get(r2.id)
                if r1.id==r2.id:
                    continue
                #if pred!=None and len(pred)>0:
                generalRel=generalRelations.get(key)
                if generalRel==None:
                    continue
                diff=set(generalRel)-set(pred)
                #print("diff: ", r1.id, r2.id, pred, diff)
                for p in pred:
                        pdesc=wikidataDAO.getWikidataProp(p)
                        if pdesc==None:
                            pdesc=PropertyStat(p, p)
                        triples.append((r1, pdesc, r2,'1'))
                        #triples.append((relation.tableCell1.tableId,pos, relation.tableCell1.colName,relation.tableCell2.colName, r1.toString(), pdesc.propId+" :"+pdesc.propName, r2.toString(), '38:1'))

                        #print(relation.tableCell1.tableId,pos, relation.tableCell1.colName,relation.tableCell2.colName, r1.toString(), pdesc.propId+" :"+pdesc.propName, r2.toString(), '38:1')
                for p in diff:
                        pdesc = wikidataDAO.getWikidataProp(p)
                        if pdesc==None:
                            pdesc=PropertyStat(p, p)
                        #print(relation.tableCell1.tableId, pos, relation.tableCell1.colName, relation.tableCell1.colName, r1.toString(), pdesc.propId+" :"+pdesc.propName, r2.toString(), '38:0')
                        triples.append((r1, pdesc, r2, '0'))
                        #triples.append((relation.tableCell1.tableId, pos, relation.tableCell1.colName, relation.tableCell1.colName, r1.toString(), pdesc.propId+" :"+pdesc.propName, r2.toString(), '38:0'))
    return triples

def getTableFeatures(table, dictFeatures):
    dictFeatures[2]=table.tableId.split(".")[1]
    dictFeatures[3]=table.nrows-table.startRows
    dictFeatures[4] = table.ncols
    dictFeatures[5] = dictFeatures[3]/dictFeatures[4]


def formatFeatures(content):
    bullets = 0
    resources = 0
    hasFormat = 0
    multipleLine = 0
    cell = BeautifulSoup(content, "html.parser")
    links = readHTML.readTableCellLinks(cell)
    # count   the    list
    bullets += len(cell.find_all("ul"))
    # count    the    enumerations
    bullets += len(cell.find_all("ol"))
    # count    font    tags
    hasFormat += len(cell.find_all("font"))
    hasFormat += len(cell.find_all("b"))
    hasFormat += len(cell.find_all("i"))
    hasFormat += len(cell.find_all("th"))
    hasFormat += len(cell.find_all("small"))
    # count    multiple - lines
    multipleLine += multipleLine + len(cell.find_all("br"))
    noLinksText = readHTML.getTagTextNoLinks(cell)
    cell.attrs = {}
    text = str(cell)
    length = len(text)

    noLinksText = [s for s in noLinksText.strings if s.strip('\n ') != '']
    noLinksText = " ".join(noLinksText)
    return {'length': length, 'bullets': bullets, 'hasFormat': hasFormat,
            'multipleLine': multipleLine, 'noLinksText': len(noLinksText)}

def getCellFeatures(tableCell1, tableCell2, dictFeatures):
    #(21 & 22) number of entities in s & o   cells
    nsubj=len(tableCell1.resources)
    nobj = len(tableCell2.resources)
    dictFeatures[21]=nsubj
    dictFeatures[22]=nobj
    # (23) ratio(21) / (22)
    dictFeatures[23] = nsubj/nobj
    # (24 & 25)  string   length in s & o    cells
    dictFeatures[24]=tableCell1.formatFeatures.get('length',0)
    dictFeatures[25]=tableCell2.formatFeatures.get('length',0)
    # (26 & 27) formatting  present in s & o   cells
    if tableCell1.formatFeatures.get('bullets',0)>0 or tableCell1.formatFeatures.get('hasFormat',0) or \
        tableCell1.formatFeatures.get('multipleLine',0):
        dictFeatures[26]=1
    else:
        dictFeatures[26] = 0

    if tableCell2.formatFeatures.get('bullets',0)>0 or tableCell2.formatFeatures.get('hasFormat',0) or \
            tableCell2.formatFeatures.get('multipleLine'):
        dictFeatures[27]=1
    else:
        dictFeatures[27]=0


def getColFeatures(relationsByCols, dictFeatures):

    subjs=[]
    objs=[]
    potRelations=[]
    for relation in relationsByCols:
        colName1=relation.tableCell1.colName
        colName2=relation.tableCell2.colName
        subjs.extend(relation.tableCell1.getResourcesIds())
        objs.extend(relation.tableCell2.getResourcesIds())
        for k, v in relation.predicates.items():
            for ki, vi in v.items():
                potRelations.append((k,ki))

    # (7) subject  column  id
    # (8) objectcolumn id
    dictFeatures[7] =relation.tableCell1.col
    dictFeatures[8] = relation.tableCell2.col
    # (9 & 10) No of entities in s & o  cols.
    # (11)  ratio: (9) / (10)
    dictFeatures[9]=nsubj=len(subjs)
    dictFeatures[10] = nobj=len(objs)
    dictFeatures[11]=nsubj/nobj
    # (12 & 13) No of  unique  entities in s & o   cols.
    dictFeatures[12] = nusubj=len(set(subjs))
    dictFeatures[13] = nuobj=len(set(objs))
    dictFeatures[14] = nusubj/nuobj
    dictFeatures[15]=len(potRelations)
    dictFeatures[16]=len(set(potRelations))


def getColNameProperties(predicate, colName1, colName2, dictFeatures):
    _colName1 = colName1.split("__")
    if len(_colName1) > 1:
        _colName1 = _colName1[1]
    else:
        _colName1=_colName1[0]
    _colName1 = _colName1.split("@")[1].replace("_", " ")
    _colName1 = _colName1.replace("**"," ")
    _colName2 = colName2.split("__")
    if len(_colName2) > 1:
        _colName2 = _colName2[1]
    else:
        _colName2=_colName2[0]
    _colName2 = _colName2.split("@")[1].replace("_", " ")
    _colName2 = _colName2.replace("**", " ")
    #print("sim:", colName1, predicate)
    #print("sim1:", colName2, predicate)
    predName=predicate.split("@")[0]
    simm1 = textProcessing.textSimilarity(_colName1, predName)
    simm2 = textProcessing.textSimilarity(_colName2, predName)

    # (28&29) string sim. for pred and s&o header
    dictFeatures[28] = simm1
    dictFeatures[29] = simm2
    # (30) maximum between (28) and (29)
    dictFeatures[30] = max([simm1, simm2])


def getColPredicateFeatures(pred, table, relationsByCols, dictFeatures):
    rowsRelation=0
    potRelations=[]
    rowRel=False
    predicate=pred.propId
    for relation in relationsByCols:
        for subj, objs in relation.predicates.items():
            for obj, preds in objs.items():
                if predicate in preds:
                    rowsRelation+=1
                    rowRel=True
                    break
            if rowRel:
                break

    for relation in relationsByCols:
        for subj, objs in relation.predicates.items():
            for obj, preds in objs.items():
                if predicate in preds:
                   potRelations.append((subj,obj))

    # (17) Normalized unique subject count
    dictFeatures[17]=pred.uniqSubj/wikidataDAO.PRED_MAX_SUBJ
    dictFeatures[18] = pred.uniqObj/wikidataDAO.PRED_MAX_OBJ
    dictFeatures[19] = pred.times/wikidataDAO.PRED_MAX_INSTA

    if dictFeatures[19]>0:
        dictFeatures[20] = dictFeatures[18]/dictFeatures[19]
    else:
        dictFeatures[20]=0
        print("Feature 19 0: ",pred.propName)

    # (31) No of rows where relation hold
    # (32) ratio: (31)/(3)
    dictFeatures[31]=rowsRelation
    dictFeatures[32] = rowsRelation/(table.nrows-table.startRows)
    # (33) No of potential relations held
    # (34) ratio: (33)/(15)
    # (35) No of unique potential relations held
    # (36) ratio: (35)/(16)
    dictFeatures[33] = len(potRelations)
    dictFeatures[34] = dictFeatures[33]/ dictFeatures[15]
    dictFeatures[35] = len(set(potRelations))
    dictFeatures[36] = dictFeatures[35]/ dictFeatures[16]


def getTotalRelations(relationsByTable):
    totalRelations=0
    for tableId, colRelations in relationsByTable.items():
        for col, relations in colRelations.items():
            for rel in relations:
                for subj, objs in rel.predicates.items():
                    for obj, preds in objs.items():
                        totalRelations+=1
    return totalRelations


def extractTriples(tableId):
    table=extractTableFile(tableId)
    colHeaders=table.colHeaders
    if len(colHeaders)==0:
        return None
    relationsByCells={}
    generalRelations={}
    relationsByCells=extractTableResources(table)
    generalRelations, relationsByTable=extractRelations(relationsByCells)
    out=""
    dictFeatures={i:0 for i in range(1,40)}
    getTableFeatures(table, dictFeatures)
    totalRel=getTotalRelations(relationsByTable)
    dictFeatures[6]=totalRel

    for tableId, colRelations in relationsByTable.items():
        for cols, relations in colRelations.items():
               getColFeatures(relations, dictFeatures)
               for rel in relations:
                cell1=rel.tableCell1
                cell2=rel.tableCell2
                ##CELL FEATURES
                getCellFeatures(cell1, cell2, dictFeatures)
                pos = str(table.startRows)+":"+str(cell1.row) + ":" + str(cell1.col) + ":" + str(
                    cell2.col)
                triples=getTriples(rel, generalRelations)
                if triples!=None:
                    for t in triples:
                        subj=t[0]
                        pred=t[1]
                        obj=t[2]
                        exist=t[3]
                        subjId=subj.id
                        objId=obj.id
                        propId=pred.propId
                        ##COL PREDICATE FEATURES
                        getColPredicateFeatures(pred, table, relations, dictFeatures)
                        getColNameProperties(pred.propName, cell1.colName, cell2.colName, dictFeatures)
                        propName=pred.propName
                        domain=str(wikidataDAO.isInDomain(subjId, propId))
                        rangeC = str(wikidataDAO.isInRange(objId, propId))
                        if "protag_article" in cell1.colName:
                            dictFeatures[37]=1
                        dictFeatures[38] = exist
                        dictFeatures[39]=domain
                        dictFeatures[40]=rangeC

                        noLinksSubj = str(cell1.formatFeatures.get('noLinksText', 0))
                        noLinksObj = str(cell2.formatFeatures.get('noLinksText', 0))
                        dictFeatures[41] =noLinksObj
                        dictFeatures[42] = noLinksSubj

                        #print(subj, pred, obj)
                        outf=""
                        for k in sorted(list(dictFeatures.keys())):
                            outf+=str(k)+":"+str(dictFeatures.get(k))+"\t"

                        out+=outf+cell1.tableId+"\t"+cell1.tableId+"\t"+pos+"\t"+cell1.colName+"\t"+cell2.colName+"\t"+subj.toString()+"\t"+propId+" :"+propName+"\t"+\
                                  obj.toString()+"\t"+exist+"\n"
    #out+="RESULT"+tableId+"\t"+str(generalRelations)+"\n"
    return out+"RESULT"+tableId+"\t"+str(generalRelations)+"\n"



def readDocuments(input=0):
    # for each document that we want to process,
    path = FOLDER_JSON_FILES
    files = os.listdir(path)
    #file=open('triples.csv', "r")
    #lines=file.readlines()
    #files=['1034127.1','367622.1','273353.3','448838.1','255639.7','1041364.1','33723.9','443678.1','173365.3','640119.1','1036693.1','557384.1','460155.1','1021863.1','238072.1','371692.1','655003.1','580309.2','909241.1','793266.5','354745.1','1036693.1','1036693.1','1036693.1','425690.3','745794.1','566590.2','1022350.1','637057.6','479435.5','578427.1','718759.1','306879.1','909980.2','327008.1','22527.2','452879.1','792062.2','223391.1','817062.1','314268.1','486787.1','586356.1','770966.2','364908.1','701502.1','722908.1','144360.1','526853.1','1028562.1','1036693.1','194891.1','172270.1','498220.1','460155.1','808448.1','142587.1','15273.1','503745.1','817062.1','748899.1','584380.1','973243.1','310788.1','494365.1','265986.1','34271.2','974726.1','265894.1','823017.1','863877.6','382284.2','349181.1','983613.1','178378.1','1036693.1','70681.8','253846.1','631826.1','237929.4','940881.3','657570.1','863533.1','526267.1','510672.1','872010.1','95607.1','272722.2','199148.1','881653.1','276290.1','1013189.1','179556.1','613057.1','184945.1','388557.1','904416.1','788135.1','345263.15','310788.1']
    for file in files:
        yield file.replace("_",".").replace(".json","")

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processDocuments(tableId):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", tableId)
    result = extractTriples(tableId)
    yield result


if __name__ == '__main__':

    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/data_json/tablesJson"#params.get("json_files")
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillData()
    wikidataDAO.fillDomainRange()
    textProcessing=TextProcessing()
    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)
    # One process combines the results into a file.
    pipeline.add(ResultCombiner('triples0.out.gz','predByCols.out.gz'))
    pipeline.run(100)

