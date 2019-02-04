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

def emptyRow(cells):
    ncells=len(cells)
    emptyCells=0
    for cell in cells:
        if cell=='<td></td>':
            emptyCells+=1
    if emptyCells==ncells:
        return True
    else:
        return False

def extractTableResources(table):
    if table.htmlMatrix==None:
        return {}
    matrix=np.array(table.htmlMatrix)
    colHeaders=table.colHeaders
    relations={}
    matrixCells=np.array([[None]*len(matrix[0])]*len(matrix))
    for row in range(table.startRows,len(matrix)):
        if emptyRow(matrix[row]):
            table.nrows-=1
            continue
        for col in range(len(matrix[0])):
            colName=colHeaders[col]
            resources = extractCellResources(matrix[row][col])
            #if len(resources)>0:
            tableCell = TableCell(table.tableId, row, col, colName)
            if len(resources)>0:
                tableCell.setResources(resources)
                format = formatFeatures(matrix[row][col])
                tableCell.setFormatFeatures(format)
            matrixCells[row][col]=tableCell
            #else:
            #matrixCells[row][col] = None


    resourceArticle = extractArticleResource(table.articleTitle)
    if resourceArticle is not None:
        for col2 in range(len(matrixCells[0])):
            colName1='protag_article@3'
            colName2 = colHeaders[col2]
            key=colName1+"##"+colName2
            #print('colName1, colName2', colName1, colName2, key)
            for row in range(table.startRows, len(matrix)):
                cell1 = TableCell(table.tableId, row, -1, colName1)
                cell1.setFormatFeatures({})
                cell1.setResources([resourceArticle])
                cell2 = matrixCells[row][col2]
                if cell2==None:
                    continue
                res2 = cell2.resources
                #if len(res2)>0:
                relation = RelationCell(cell1, cell2)
                if relations.get(key) == None:
                    relations[key] = [relation]
                else:
                    relations[key].append(relation)

    for col1 in range(len(matrix[0])):
        colName1=colHeaders[col1]
        for col2 in range(col1+1, len(matrix[0])):
            colName2 = colHeaders[col2]
            dictCols = {colName1: col1, colName2: col2}
            sortedCols =[colName1, colName2]
            sortedCols.sort()
            key = "##".join(sortedCols)
            splitKey=key.split("##")
            newColName1=splitKey[0]
            newColName2=splitKey[1]
            posCol1=dictCols.get(newColName1)
            posCol2 = dictCols.get(newColName2)
            #print('colName1, colName2', colName1, colName2, key)
            for row in range(table.startRows, len(matrix)):
                if matrix[row][posCol1]==matrix[row][posCol2]:
                    continue
                cell1=matrixCells[row][posCol1]
                cell2 = matrixCells[row][posCol2]
                if cell1==None or cell2==None:
                    continue
                res1=cell1.resources
                res2=cell2.resources
                if len(res1)>0 or len(res2)>0:
                    relation=RelationCell(cell1, cell2)
                    if relations.get(key)==None:
                        relations[key]=[relation]
                    else:
                        relations[key].append(relation)

    return relations

def extractRelations(colRelations):
    allRelations=[]
    newTableRelations={}
    dictColRelations={}
    generalRelations={}

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
                            #if 'Q216517' in res1.id and 'Q472750' in res2.id:
                            #    predicates=['P8888888']
                            #else:
                            predicates= wikidataDAO.getRelations(res1.id, res2.id) #['P'+str(int(random.uniform(2,10)))]##
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
    return generalRelations,dictColRelations

def getNewTriples(tableId,relation, generalPredicates):
    if generalPredicates==None or len(generalPredicates)==0:
        return []
    res1 = relation.tableCell1.resources
    res2 = relation.tableCell2.resources
    pos=str(relation.tableCell1.row)+":"+str(relation.tableCell1.col)+":"+str(relation.tableCell2.col)
    triples=[]

    for r1 in res1:
        obj = relation.predicates.get(r1.id)
        if obj != None:
            for r2 in res2:
                preds = obj.get(r2.id)
                if r1.id==r2.id:
                    continue
                #print("Preds, generalP: ", tableId, r1.url, r2.url, preds, generalPredicates)
                if preds!=None:
                    diff=set(generalPredicates)-set(preds)
                else:
                    diff=generalPredicates
                if len(diff)==0:
                    continue
                listPreds=[]
                if len(diff)>0:
                    for p in diff:
                        pdesc=wikidataDAO.getWikidataProp(p)
                        if pdesc==None:
                            pdesc=PropertyStat(p, p)
                        listPreds.append(pdesc)
                    tripleFeatures=TripleFeatures(tableId, pos,  r1, r2, listPreds)
                    triples.append(tripleFeatures)
    return triples


def getTriples(tableId,relation):
    res1 = relation.tableCell1.resources
    res2 = relation.tableCell2.resources
    pos=str(relation.tableCell1.row)+":"+str(relation.tableCell1.col)+":"+str(relation.tableCell2.col)
    triples=[]
    for r1 in res1:
        obj = relation.predicates.get(r1.id)
        if obj != None:
            for r2 in res2:
                preds = obj.get(r2.id)
                if r1.id==r2.id:
                    continue
                listPreds=[]
                if len(preds)>0:
                    for p in preds:
                        pdesc=wikidataDAO.getWikidataProp(p)
                        if pdesc==None:
                            pdesc=PropertyStat(p, p)
                        listPreds.append(pdesc)
                    tripleFeatures=TripleFeatures(tableId, pos, r1, r2, listPreds)
                    triples.append(tripleFeatures)
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
            'multipleLine': multipleLine, 'noLinksText': len(noLinksText), "links":len(links)}



def getColFeatures(relationsByCols, dictFeatures):

    subjs=[]
    objs=[]
    potRelations=[]
    for relation in relationsByCols:
        subjs.extend(relation.tableCell1.getResourcesIds())
        objs.extend(relation.tableCell2.getResourcesIds())
        for k, v in relation.predicates.items():
            for ki, vi in v.items():
                if k!=ki:
                    potRelations.append((k,ki))

    # (7) subject  column  id
    # (8) objectcolumn id
    dictFeatures[7] =relation.tableCell1.col
    dictFeatures[8] = relation.tableCell2.col
    # (9 & 10) No of entities in s & o  cols.
    # (11)  ratio: (9) / (10)
    dictFeatures[9]=nsubj=len(subjs)
    dictFeatures[10] = nobj=len(objs)
    if nobj==0:
        dictFeatures[11]=nsubj
    else:
        dictFeatures[11] = nsubj/nobj
    # (12 & 13) No of  unique  entities in s & o   cols.
    dictFeatures[12] = nusubj=len(set(subjs))
    dictFeatures[13] = nuobj=len(set(objs))
    if nuobj==0:
        dictFeatures[14] = nusubj
    else:
        dictFeatures[14] = nusubj / nuobj
    dictFeatures[15]=len(potRelations)
    dictFeatures[16]=len(set(potRelations))


def getColNameFeatures(predicate, colName1, colName2, dictFeatures):
    _colName1 = colName1.split("__")
    if len(_colName1) > 1:
        _colName1 = _colName1[1]
    else:
        _colName1=_colName1[0]
    _colName1 = _colName1.split("@")[0].replace("_", " ")
    _colName1 = _colName1.replace("**"," ")
    _colName2 = colName2.split("__")
    if len(_colName2) > 1:
        _colName2 = _colName2[1]
    else:
        _colName2=_colName2[0]
    _colName2 = _colName2.split("@")[0].replace("_", " ")
    _colName2 = _colName2.replace("**", " ")
    #print("sim:", colName1, predicate)
    #print("sim1:", colName2, predicate)
    if len(predicate)==0:
        _predicate="None"
    else:
        _predicate = predicate
    predName=_predicate.split("@")[0]
    #print("names:, ", _colName1, predName)
    #print("names2:, ", _colName2, predName)
    simm1 = textProcessing.textSimilarity(_colName1, predName)
    simm2 = textProcessing.textSimilarity(_colName2, predName)

    # (28&29) string sim. for pred and s&o header
    dictFeatures[28] = simm1
    dictFeatures[29] = simm2
    # (30) maximum between (28) and (29)
    dictFeatures[30] = max([simm1, simm2])

def getColPredicateFeatures(pred, relationsByCols, dictFeatures):
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
    if dictFeatures[3]==0:
        print("Feature 3 0: ", pred.propName)
        dictFeatures[32]=0
    else:
        dictFeatures[32] = rowsRelation/dictFeatures[3]
    # (33) No of potential relations held
    # (34) ratio: (33)/(15)
    # (35) No of unique potential relations held
    # (36) ratio: (35)/(16)
    dictFeatures[33] = len(potRelations)
    if dictFeatures[15]==0:
        print("Feature 15 0: ", pred.propName)
        dictFeatures[34] = 0
    else:
        dictFeatures[34] = dictFeatures[33]/ dictFeatures[15]

    if dictFeatures[16]==0:
        print("Feature 16 0: ", pred.propName)
    else:
        dictFeatures[36] = dictFeatures[35] / dictFeatures[16]
    dictFeatures[35] = len(set(potRelations))


def getCellFeatures(tableCell1, tableCell2, dictFeatures):
    #(21 & 22) number of entities in s & o   cells
    nsubj=len(tableCell1.resources)
    nobj = len(tableCell2.resources)
    dictFeatures[21]=nsubj
    dictFeatures[22]=nobj
    # (23) ratio(21) / (22)
    if nobj==0:
        dictFeatures[23] = nsubj
    else:
        dictFeatures[23] = nsubj / nobj
    # (24 & 25)  string   length in s & o    cells
    dictFeatures[24]=tableCell1.formatFeatures.get('length',0)
    dictFeatures[25]=tableCell2.formatFeatures.get('length',0)
    # (26 & 27) formatting  present in s & o   cells
    if tableCell1.formatFeatures.get('bullets',0)>0 or tableCell1.formatFeatures.get('hasFormat',0)>0 or \
        tableCell1.formatFeatures.get('multipleLine',0)>0:
        dictFeatures[26]=1
    else:
        dictFeatures[26] = 0

    if tableCell2.formatFeatures.get('bullets',0)>0 or tableCell2.formatFeatures.get('hasFormat',0)>0 or \
            tableCell2.formatFeatures.get('multipleLine',0)>0:
        dictFeatures[27]=1
    else:
        dictFeatures[27]=0

    noLinksSubj = str(tableCell1.formatFeatures.get('noLinksText', 0))
    noLinksObj = str(tableCell2.formatFeatures.get('noLinksText', 0))
    linksSubj = str(tableCell1.formatFeatures.get('links', 0))
    linksObj = str(tableCell2.formatFeatures.get('links', 0))
    dictFeatures[41] = noLinksObj
    dictFeatures[42] = noLinksSubj
    dictFeatures[43] = linksSubj
    dictFeatures[44] = linksObj


def getTotalRelations(colRelations):
    totalRelations=0
    for col, relations in colRelations.items():
            for rel in relations:
                for subj, objs in rel.predicates.items():
                    for obj, preds in objs.items():
                        totalRelations+=1
    return totalRelations

def getClusterFeatures(featuresCluster, relations):
    rowsByPred=[]
    for relation in relations:
        row=0
        subjects=relation.tableCell1.getResourcesIds()
        objects = relation.tableCell2.getResourcesIds()
        featuresCluster.addSubjects(subjects)
        featuresCluster.addObjects(objects)
        for subj, objs in relation.predicates.items():
            for obj, preds in objs.items():
                if subj!=obj:
                    featuresCluster.addRelations([(subj, obj)])
                    for pred in preds:
                        featuresCluster.addRelationsByProperty(pred,[(subj,obj)])
                        if row==0 or pred not in rowsByPred:
                            featuresCluster.addRowsByProperty(pred)
                            rowsByPred.append(pred)
                            row=1

def extractFeatures(tables):
    dictFeaturesCluster={}
    triplesCluster=[]
    cluster=""
    for name in tables:
        try:
            cluster=name.split("#")[0]
            print('table',name)
            tableId = name.split("#")[1]

            table=extractTableFile(tableId)
            if table==None:
                print('Table not found: ', cluster, tableId)
                continue
            colHeaders=table.colHeaders
            if len(colHeaders)==0:
                continue
            relationsByCells={}
            generalRelations={}
            dictFeatures = {i: 0 for i in range(1, 44)}
            relationsByCells=extractTableResources(table)
            #print('rel by cells: ', relationsByCells)
            generalRelations, relationsByTable=extractRelations(relationsByCells)
            getTableFeatures(table, dictFeatures)
            totalRel = getTotalRelations(relationsByTable)
            dictFeatures[6] = totalRel

            for cols, relations in relationsByTable.items():

                #print("cols: ", cols)
                featuresCluster=dictFeaturesCluster.get(cols)
                if featuresCluster==None:
                    featuresCluster=FeaturesCluster(cols)
                getClusterFeatures(featuresCluster, relations)
                dictFeaturesCluster[cols]=featuresCluster
                getColFeatures(relations, dictFeatures)
                for rel in relations:
                    cell1 = rel.tableCell1
                    cell2 = rel.tableCell2
                    ##CELL FEATURES
                    getCellFeatures(cell1, cell2, dictFeatures)
                    pos = str(table.startRows) + ":" + str(cell1.row) + ":" + str(cell1.col) + ":" + str(
                        cell2.col)
                    triples = getTriples(tableId, rel)
                    #newTriples = getNewTriples(tableId, rel, generalRelations.get(cols))
                    while triples:
                        t=triples.pop()
                        featuresPred={}
                        tpreds=[p.propId for p in t.predicates]
                        newPreds=set(generalRelations.get(cols))-set(tpreds)
                        for pred in t.predicates:
                            getColPredicateFeatures(pred, relations, dictFeatures)
                            getColNameFeatures(pred.propName, cell1.colName, cell2.colName, dictFeatures)
                            propName = pred.propName
                            domain = str(wikidataDAO.isInDomain(t.subj.id, pred.propId))
                            rangeC = str(wikidataDAO.isInRange(t.obj.id, pred.propId))
                            if "protag_article" in cell1.colName or 'protag_article' in cell2.colName:
                                    dictFeatures[37] = 1
                            dictFeatures[38] = 1
                            dictFeatures[39] = domain
                            dictFeatures[40] = rangeC

                            featuresPred[pred.propId]=dictFeatures.copy()
                        for predId in newPreds:
                            pred=wikidataDAO.getWikidataProp(predId)
                            if pred==None:
                                pred=PropertyStat(predId, predId)
                            getColPredicateFeatures(pred, relations, dictFeatures)
                            getColNameFeatures(pred.propName, cell1.colName, cell2.colName, dictFeatures)
                            propName = pred.propName
                            domain = str(wikidataDAO.isInDomain(t.subj.id, pred.propId))
                            rangeC = str(wikidataDAO.isInRange(t.obj.id, pred.propId))
                            if "protag_article" in cell1.colName or 'protag_article' in cell2.colName:
                                    dictFeatures[37] = 1
                            dictFeatures[38] = 0
                            dictFeatures[39] = domain
                            dictFeatures[40] = rangeC
                            featuresPred[pred.propId]=dictFeatures.copy()
                        t.setFeatures(featuresPred)
                        t.setCols(cols)
                        t.setColName1(cell1.colName)
                        t.setColName2(cell2.colName)
                        triplesCluster.append(t)


        except Exception as ex:
            traceback.print_exc()
            print("Error: ", name)
            continue
    dictClusterFeaturesByCols={}
    dictClusterPropertyByCols = {}
    for cols, featuresCluster in dictFeaturesCluster.items():
            featuresCluster = dictFeaturesCluster.get(cols)
            featuresCluster.calcFeatures()
            #print('cluster: D', cols, featuresCluster.dictFeatures)
            propertiesCluster=featuresCluster.getProperties()
            #print('cluster P: ', cols,  propertiesCluster)
            dictClusterFeaturesByCols[cols]=featuresCluster.dictFeatures.get(cols)
            dictClusterPropertyByCols[cols]=propertiesCluster
    dictFeaturesCluster.clear()
    del relationsByTable
    out=""
    while triplesCluster:
        triple=triplesCluster.pop()
        tableId=triple.tableId
        featuresByPred=triple.features
        triplePreds=list(featuresByPred.keys())
        clusterPreds=dictClusterPropertyByCols.get(triple.cols)

        newPreds=set(clusterPreds)-set(triplePreds)

        print("properties", triple.cols, clusterPreds, triplePreds, newPreds)
        copyFeatures={}
        while triplePreds:
            pred=triplePreds.pop()
            _pred=wikidataDAO.getWikidataProp(pred)
            if _pred==None:
                pred=PropertyStat(pred, pred)
            else:
                pred=_pred
            features=featuresByPred.get(pred.propId)
            #print("Features:  ", pred.propId, features)
            if len(triplePreds)==0:
                copyFeatures=features.copy()
            copyFeatures.copy()
            featuresCluster=dictClusterFeaturesByCols.get(triple.cols).get(pred.propId)
            #print("Features cluster: ", dictClusterFeaturesByCols, featuresCluster)
            allFeatures=features.copy()
            allFeatures.update(featuresCluster)
            outf = ""
            for k in sorted(list(allFeatures.keys())):
                outf += str(k) + ":" + str(allFeatures.get(k)) + "\t"
            out += outf + cluster + "\t" + triple.tableId + "\t" + triple.pos + "\t" + triple.colName1 + "\t" + triple.colName2 + "\t" + triple.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                   triple.obj.toString() + "\n"
        while newPreds:
            #print("New preds", newPreds)
            pred = newPreds.pop()
            _pred = wikidataDAO.getWikidataProp(pred)
            if _pred == None:
                pred = PropertyStat(pred, pred)
            else:
                pred=_pred
            featuresCluster=dictClusterFeaturesByCols.get(triple.cols).get(pred.propId)
            #print("New preds, features:", cluster, triple.cols, pred.propId)
            allFeatures=copyFeatures
            allFeatures.update({31: 0, 32: 0, 33: 0, 34: 0, 35: 0, 36: 0})
            allFeatures.update(featuresCluster)
            getColNameFeatures(pred.propName, triple.colName1, triple.colName2, allFeatures)
            propName = pred.propName
            domain = str(wikidataDAO.isInDomain(triple.subj.id, pred.propId))
            rangeC = str(wikidataDAO.isInRange(triple.obj.id, pred.propId))
            if "protag_article" in triple.colName1 or 'protag_article' in triple.colName2:
                allFeatures[37] = 1
            allFeatures[38] = 2
            allFeatures[39] = domain
            allFeatures[40] = rangeC
            outf = ""
            for k in sorted(list(allFeatures.keys())):
                outf += str(k) + ":" + str(allFeatures.get(k)) + "\t"
            out += outf + cluster + "\t" + triple.tableId + "\t" + triple.pos + "\t" + triple.colName1 + "\t" + triple.colName2 + "\t" + triple.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                   triple.obj.toString() + "\n"
    return out+"RESULT"+cluster+"\t"+str(dictClusterPropertyByCols)+"\n"


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
                if int(_line[0])>=5000:
                    if cluster!=_line[0] and cluster!='':
                        #out=extractFeatures(clustert)
                        #if out=="":
                        #    print("Cluster no relations found: ", cluster)
                        #else:
                        #    fout.write(out)
                        listClusters.append(clustert)
                        clustert=[_line[0]+"#"+_line[1]]
                        cluster=_line[0]
                    else:
                        cluster=_line[0]
                        clustert.append(_line[0]+"#"+_line[1])
    if len(clustert)>0:
        listClusters.append(clustert)
        #out=extractFeatures(clustert)
        #if out == "":
            #    print("Cluster no relations found: ", cluster)
            #else:
            #    fout.write(out)

    print("Total custers:", len(listClusters))
    for cluster in listClusters:
        print('len cluster', len(cluster))
        yield cluster
    #lines=file.readlines()
    #files=['1#524269.20','1#708040.3']
    #yield files #.replace("_",".").replace(".json","")

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processClusters(tableId):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    #print("File: ", tableId)
    result = extractFeatures(tableId)
    yield result


if __name__ == '__main__':

    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/data_json/tablesJson"  # params.get("json_files")
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillData()
    wikidataDAO.fillDomainRange()
    textProcessing = TextProcessing()
    FILE_CLUSTER=args[0]
    fileTriplesOut=args[1]
    fileClusterRelations=args[2]
    pipeline = Pipey.Pipeline()
    pipeline.add(process)
    pipeline.add(processClusters, 4)
    pipeline.add(ResultCombiner(fileTriplesOut, fileClusterRelations))
    pipeline.run(100)

