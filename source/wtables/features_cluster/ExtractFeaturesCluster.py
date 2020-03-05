import traceback

from wtables.features_cluster.FeaturesCluster import FeaturesCluster
from wtables.features_cluster.FeaturesTableCell import TableCell
from wtables.features_cluster.FeaturesTriple import TripleFeatures
from wtables.features_cluster.RelationCell import RelationCell
from wtables.preprocessing import ReadHTML as readHTML
from wtables.preprocessing.TextProcessing import TextProcessing
from wtables.schema.Article import *
from wtables.schema.Resource import Resource
from wtables.wikidata_db.WikidataDAO import *


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
                    resources[wd]=resource
    #print("List resources:", resources)
    resources=list(resources.values())
    return resources

def extractArticleResource(articleTitle):
    #Convert article of title to Link and get Wikidata ID
    _link = wikiLink(articleTitle)
    if _link != None and _link != "":
        wd= wikidataDAO.getWikidataID(_link)
        if wd!="" and wd!=None:
            resource=Resource(_link)
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

def extractTableResources(table):
    #Fill a table matrix with resources for each cell and extract relations between a pair cells.
    #Creates a dictionary to return relations by pair of columns.
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
            if (matrix[row][col])==None:
                print("Cell None:", table.tableId, matrix[row][col], row, col)
                continue
            resources = extractCellResources(matrix[row][col])
            #if len(resources)>0:
            tableCell = TableCell(table.tableId, row, col, colName)
            #if len(resources)>0:
            tableCell.setResources(resources)
            try:
                    format = formatFeatures(matrix[row][col])
                    tableCell.setFormatFeatures(format)
            except Exception as ex:
                    print('Error format')
                    continue
            matrixCells[row][col]=tableCell
            #else:
            #matrixCells[row][col] = None

    #Create relations between cells and title's article
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
                if len(res2)==1:
                    relation = RelationCell(cell1, cell2)
                    if relations.get(key) == None:
                        relations[key] = [relation]
                    else:
                        relations[key].append(relation)

    # Create relations between a pair of cells that contain resources in table matrix.
    for col1 in range(len(matrix[0])):
        colName1=colHeaders[col1]
        for col2 in range(col1+1, len(matrix[0])):
            #Col names are sortered, to avoid incorrect property directions in a cluster.
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
                if len(res1)==1 or len(res2)==1:
                    relation=RelationCell(cell1, cell2)
                    if relations.get(key)==None:
                        relations[key]=[relation]
                    else:
                        relations[key].append(relation)

    return relations

def extractRelations(colRelations):
    # Extract predicates from Wikidata, for each relation.

    allRelations=[]
    newTableRelations={}
    dictColRelations={}
    generalRelations={}


    for cols, relations in colRelations.items():
            relationsResources=[]
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
                            predicates=wikidataDAO.getRelations(res1.id, res2.id) # ['P'+str(int(random.uniform(2,10)))]##
                            #print('relation: ', res1.id, res2.id, predicates)
                            allPredicates.extend(predicates)
                            if relation.predicates.get(res1.id)==None:
                                relation.predicates[res1.id]={res2.id:predicates}
                            else:
                                relation.predicates[res1.id][res2.id]= predicates

                if len(resources1)>0 and len(resources2)>0:
                    relationsResources.append(relation)
                    relationsByCols=generalRelations.get(cols)
                    if len(allPredicates) > 0:
                        if relationsByCols is None:
                            generalRelations[cols]=list(set(allPredicates))
                        else:
                            relationsByCols=set(relationsByCols)
                            generalRelations[cols]=list(relationsByCols.union(set(allPredicates)))
            #Save relations by key: cols
            dictColRelations[cols]=relationsResources[:]
    return generalRelations,dictColRelations


def getTriples(tableId,relation):
    #Extract triples by all resources in a relations, means that a relation could have more than a pair of entities.
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
    #Fill features by table
    dictFeatures[2]=table.tableId.split(".")[1]
    dictFeatures[3]=table.nrows-table.startRows
    dictFeatures[4] = table.ncols
    dictFeatures[5] = dictFeatures[3]/dictFeatures[4]


def formatFeatures(content):
    #Extrac format features from cell (content)
    bullets = 0
    resources = 0
    hasFormat = 0
    multipleLine = 0
    try:
        #print(content)
        bsoup = BeautifulSoup(content)
        #print(bsoup)
        if "<td" in content:
            cell = bsoup.find("td")
        else:
            cell = bsoup.find("th")
        #print(cell)
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
        cspan=cell.get('colspan')
        rspan=cell.get('rowspan')
        if cspan!=None:
            cspan=1
        else:
            cspan=0
        if rspan!=None:
            rspan=1
        else:
            rspan=0
        cell.attrs = {}
        text = str(cell)
        length = len(text)

        noLinksText = [s for s in noLinksText.strings if s.strip('\n ') != '']
        noLinksText = " ".join(noLinksText)
        if cspan ==1 or rspan==1:
            hasSpan=1
        else:
            hasSpan=0

        return {'length': length, 'bullets': bullets, 'hasFormat': hasFormat,
            'multipleLine': multipleLine, 'noLinksText': len(noLinksText), "links":len(links), "hasSpan":hasSpan}
    except Exception as ex:
        raise Exception("Error html cell")
        #return {'length': 0, 'bullets': 0, 'hasFormat': 0,
        #        'multipleLine': 0, 'noLinksText': 0, "links": 0, "hasSpan": 0}



def getColTableFeatures(relationsByCols, dictFeatures):
    #Extract column features from relations by pair of cols.
    subjs=[]
    objs=[]
    subjs2=[]
    objs2 = []
    potRelations=[]
    rel=None
    #print(len(relationsByCols))
    for relation in relationsByCols:
        rel=relation
        resSubj=relation.tableCell1.getResourcesIds()
        resObj=relation.tableCell2.getResourcesIds()
        subjs.extend(resSubj)
        objs.extend(resObj)
        if len(resSubj)>0 and len(resObj)>0:
            subjs2.extend(resSubj)
            objs2.extend(resObj)
        for k, v in relation.predicates.items():
            for ki, vi in v.items():
                if k!=ki:
                    potRelations.append((k,ki))

    # (7) subject  column  id
    # (8) objectcolumn id
    #print(rel.tableCell1.colName)
    dictFeatures[7] =rel.tableCell1.col
    dictFeatures[8] = rel.tableCell2.col
    # (9 & 10) No of entities in s & o  cols.
    # (11)  ratio: (9) / (10)
    dictFeatures[9]=nsubj=len(subjs)
    dictFeatures[10] = nobj=len(objs)

    if nobj==0:
        #print("nobj 0: ", nobj)
        dictFeatures[11]=nsubj
    else:
        dictFeatures[11] = nsubj/nobj
    # (12 & 13) No of  unique  entities in s & o   cols.
    dictFeatures[12] = nusubj=len(set(subjs))
    dictFeatures[13] = nuobj=len(set(objs))
    if nuobj==0:
        #print("nuobj: 0", nuobj)
        dictFeatures[14] = nusubj
    else:
        dictFeatures[14] = nusubj / nuobj
    dictFeatures[15]=len(potRelations)
    dictFeatures[16]=len(set(potRelations))

    # dictFeatures[50] = nsubj2 = len(subjs2)
    # dictFeatures[51] = nobj2 = len(objs2)
    # if nobj2>0:
    #     dictFeatures[52] = nsubj2/nobj2
    # else:
    #     #print("nobj2 0: ", nobj2)
    #     dictFeatures[52] = nsubj2
    #
    # dictFeatures[53] = nusubj2 = len(set(subjs2))
    # dictFeatures[54] = nuobj2 = len(set(objs2))
    # if nuobj2>0:
    #     dictFeatures[55] = nusubj2/nuobj2
    # else:
    #     #print("nuobj2 0: ", nuobj2)
    #     dictFeatures[55] = nusubj2


def getColNameFeatures(predicate, colName1, colName2, dictFeatures):
    #Extract column name features, specifically text simmilarity between column names and predicate
    """_colName1 = colName1.split("__")
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
    predName=_predicate.split("@")[0]"""
    #print("names:, ", _colName1, predName)
    #print("names2:, ", _colName2, predName)
    simm1 = textProcessing.textSimilarity(colName1, predicate)
    simm2 = textProcessing.textSimilarity(colName2, predicate)

    # (28&29) string sim. for pred and s&o header
    dictFeatures[28] = simm1
    dictFeatures[29] = simm2
    # (30) maximum between (28) and (29)
    dictFeatures[30] = max([simm1, simm2])

def getPredicateFeatures(pred, dictFeatures):
    #Extract features from stats of predicate

    # (17) Normalized unique subject count
    #if "-1" in pred.propId:
    #    _pred=wikidataDAO.getWikidataProp(pred.propId.replace("-1",""))
    #else:
    _pred=pred
    dictFeatures[17] = _pred.uniqSubj / wikidataDAO.PRED_MAX_SUBJ
    dictFeatures[18] = _pred.uniqObj / wikidataDAO.PRED_MAX_OBJ
    dictFeatures[47] = _pred.maxSubj
    dictFeatures[48] = _pred.maxObj

    dictFeatures[19] = _pred.times / wikidataDAO.PRED_MAX_INSTA
    if dictFeatures[19]>0:
        dictFeatures[20] = dictFeatures[18]/dictFeatures[19]
    else:
        dictFeatures[20]=0
        print("Feature 19 0: ",_pred.propName)



def getColPredicateTableFeatures(pred, relationsByCols, dictFeatures):
    #Extract features for predicate in a set of relations.
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

    dictFeatures[35] = len(set(potRelations))

    if dictFeatures[16]==0:
        print("Feature 16 0: ", pred.propName)
    else:
        dictFeatures[36] = dictFeatures[35] / dictFeatures[16]




def getCellFeatures(tableCell1, tableCell2, dictFeatures):
    #Extract numeric features by tableCell. Protagonist article cell will have 0 in format features.

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
    # (26 & 27) format
    # ting  present in s & o   cells
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
    linksSubj = str(tableCell1.formatFeatures.get('links', -1))
    linksObj = str(tableCell2.formatFeatures.get('links', 0))
    hasSpanSubj= str(tableCell1.formatFeatures.get('hasSpan', 0))
    hasSpanObj = str(tableCell2.formatFeatures.get('hasSpan', 0))
    dictFeatures[41] = noLinksSubj
    dictFeatures[42] = noLinksObj
    dictFeatures[43] = linksSubj
    dictFeatures[44] = linksObj
    dictFeatures[45]=hasSpanSubj
    dictFeatures[46] = hasSpanObj



def getTotalRelations(colRelations):
    #Count total relations by pair of cols in a table.
    totalRelations=0
    for col, relations in colRelations.items():
            for rel in relations:
                for subj, objs in rel.predicates.items():
                    totalRelations+=len(objs)
    return totalRelations

def getClusterFeatures(featuresCluster, relations):
    #Extract cluster features, such as number of predicates by pair of columns.
    rowsByPred=[]
    for relation in relations:
        row=0
        subjects=relation.tableCell1.getResourcesIds()
        objects = relation.tableCell2.getResourcesIds()
        if len(subjects)>0 and len(objects)>0:
            #print("s ", subjects)
            #print("o ",objects)
            featuresCluster.addSubjects(subjects)
            featuresCluster.addObjects(objects)
        for subj, objs in relation.predicates.items():
            for obj, preds in objs.items():
                if subj!=obj:
                    featuresCluster.addRelations([(subj, obj)])
                    if len(preds)>0:
                        featuresCluster.addRelationsWithPredicate(1)
                    for pred in preds:
                        featuresCluster.addRelationsByProperty(pred,[(subj,obj)])
                        if row==0 or pred not in rowsByPred:
                            featuresCluster.addRowsByProperty(pred)
                            rowsByPred.append(pred)
                            row=1

def extractFeatures(tables):
    #Generate input for getTriplesCluster method.
    # Store in dicts reations by tables, predicates and features by table and predicates by cluster.
    dictFeaturesCluster={}
    triplesCluster=[]
    cluster=""
    totalRows=0
    ntables=0
    relationsByTables={}
    predsByTables={}
    featuresTable={}
    for name in tables:
        try:
            ntables+=1
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
            dictFeatures = {i: 0 for i in range(1, 5)}
            relationsByCells=extractTableResources(table)
            #print('rel by cells: ', relationsByCells)
            generalRelations, relationsByTable=extractRelations(relationsByCells)
            getTableFeatures(table, dictFeatures)
            totalRel = getTotalRelations(relationsByTable)
            totalRows+=dictFeatures[3]
            dictFeatures[6]=totalRel
            relationsByTables[tableId] = relationsByTable
            featuresTable[tableId]=dictFeatures.copy()

            for cols, relations in relationsByTable.items():
                #print("cols: ", cols)
                featuresCluster=dictFeaturesCluster.get(cols)
                if featuresCluster==None:
                    featuresCluster=FeaturesCluster(cols)
                #print("cooolsss: ", cols)
                getClusterFeatures(featuresCluster, relations)
                #UPDATE CLUSTER FEATURES
                dictFeaturesCluster[cols]=featuresCluster
            relationsByTables[tableId] = relationsByTable.copy()
            predsByTables[tableId] = generalRelations.copy()
            del relationsByTable
            del generalRelations
        except Exception as ex:
            traceback.print_exc()
            print("Error: ", name)
            continue
    print('Finnnnn')
    dictClusterFeaturesByCols={}
    dictClusterPropertyByCols = {}
    for cols, featuresCluster in dictFeaturesCluster.items():
            featuresCluster = dictFeaturesCluster.get(cols)
            featuresCluster.calcFeatures()
            #print('cluster: D', cols, featuresCluster.dictFeatures)
            propertiesCluster=featuresCluster.getProperties()
            #print('cluster P: ', cols,  propertiesCluster)
            dictClusterFeaturesByCols[cols]=featuresCluster.dictFeatures.get(cols)
            #dictClusterFeaturesByCols[cols]['110'] =totalRows
            dictClusterPropertyByCols[cols]=propertiesCluster
    dictFeaturesCluster.clear()
    return relationsByTables, featuresTable, predsByTables, dictClusterFeaturesByCols, dictClusterPropertyByCols, totalRows

def getTriplesCluster(tables):
    cluster=tables[0].split("#")[0]
    relationsByTables, featuresTable, predsByTables, dictClusterFeaturesByCols, dictClusterPropertyByCols, totalRows=extractFeatures(tables)
    out=""
    keyst=list(relationsByTables.keys())
    for table in keyst:
        numberTables=len(keyst)

        relationsTable=relationsByTables.get(table)
        dictFeatures = {i: 0 for i in range(1, 55)}
        #TABLE FEATURES
        dictFeatures.update(featuresTable.get(table))
        for cols, relations in relationsTable.items():
            #COL FEATURES
            if len(relations)==0:
                continue
            getColTableFeatures(relations, dictFeatures)
            for rel in relations:
                cell1 = rel.tableCell1
                cell2 = rel.tableCell2
                ##CELL FEATURES
                getCellFeatures(cell1, cell2, dictFeatures)
                triples = getTriples(table, rel)
                tablePreds = predsByTables.get(table).get(cols)
                if tablePreds is None:
                    tablePreds = []
                clusterPreds = set(dictClusterPropertyByCols.get(cols)) - set(tablePreds)
                while triples:
                    t = triples.pop()
                    featuresPred = {}
                    tpreds = [p.propId for p in t.predicates]
                    if tpreds is None:
                        tpreds=[]

                    newPreds = set(tablePreds) - set(tpreds)
                    for pred in t.predicates:
                        getColPredicateTableFeatures(pred, relations, dictFeatures)
                        getColNameFeatures(pred.propName, cell1.colName, cell2.colName, dictFeatures)
                        getPredicateFeatures(pred, dictFeatures)
                        propName = pred.propName
                        domain = str(wikidataDAO.isInDomain(t.subj.id, pred.propId))
                        rangeC = str(wikidataDAO.isInRange(t.obj.id, pred.propId))
                        if "protag_article" in cell1.colName or 'protag_article' in cell2.colName:
                            dictFeatures[37] = 1
                        dictFeatures[38] = 1
                        dictFeatures[39] = domain
                        dictFeatures[40] = rangeC
                        featuresCluster = dictClusterFeaturesByCols.get(cols).get(pred.propId)
                        allFeatures = dictFeatures.copy()
                        allFeatures.update(featuresCluster)

                        objectsBySubj = wikidataDAO.getObjBySubjProp(t.subj.id, pred.propId)
                        allFeatures[49] = str(objectsBySubj)
                        subjByObj = wikidataDAO.getSubjByObjProp(t.obj.id, pred.propId)
                        allFeatures[50] = str(subjByObj)

                        allFeatures[60] = totalRows
                        allFeatures[61] = allFeatures[58]/allFeatures[56]
                        allFeatures[62] = allFeatures[59]/totalRows
                        allFeatures[63] = allFeatures[57] / allFeatures[55]
                        if "-1" in pred.propName:
                            allFeatures[64]=1
                        else:
                            allFeatures[64] = 1
                        allFeatures[65]=numberTables



                        #objectsBySubj = wikidataDAO.getObjBySubjProp(t.subj.id, pred.propId)
                        #allFeatures[49] = str(len(objectsBySubj))
                        outf = ""
                        for k in sorted(list(allFeatures.keys())):
                            outf += str(k) + ":" + str(allFeatures.get(k)) + "\t"

                        out += outf + cluster + "\t" + t.tableId + "\t" + t.pos + "\t" + cell1.colName + "\t" + cell2.colName + "\t" + t.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                            t.obj.toString() + "\n"
                        #fout.write(
                        #   outf + cluster + "\t" + t.tableId + "\t" + t.pos + "\t" + cell1.colName + "\t" + cell2.colName + "\t" + t.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                        #    t.obj.toString() + "\n")

                    for predId in newPreds:
                        pred = wikidataDAO.getWikidataProp(predId)
                        if pred == None:
                            pred = PropertyStat(predId, predId)
                        getColPredicateTableFeatures(pred, relations, dictFeatures)
                        getColNameFeatures(pred.propName, cell1.colName, cell2.colName, dictFeatures)
                        getPredicateFeatures(pred, dictFeatures)
                        propName = pred.propName
                        domain = str(wikidataDAO.isInDomain(t.subj.id, pred.propId))
                        rangeC = str(wikidataDAO.isInRange(t.obj.id, pred.propId))
                        if "protag_article" in cell1.colName or 'protag_article' in cell2.colName:
                            dictFeatures[37] = 1
                        dictFeatures[38] = 0
                        dictFeatures[39] = domain
                        dictFeatures[40] = rangeC

                        featuresCluster = dictClusterFeaturesByCols.get(cols).get(pred.propId)
                        allFeatures = dictFeatures.copy()
                        allFeatures.update(featuresCluster)
                        objectsBySubj = wikidataDAO.getObjBySubjProp(t.subj.id, pred.propId)
                        allFeatures[49] = str(objectsBySubj)
                        subjByObj = wikidataDAO.getSubjByObjProp(t.obj.id, pred.propId)
                        allFeatures[50] = str(subjByObj)

                        allFeatures[60] = totalRows
                        allFeatures[61] = allFeatures[58] / allFeatures[56]
                        allFeatures[62] = allFeatures[59] / totalRows
                        allFeatures[63] = allFeatures[57] / allFeatures[55]
                        if "-1" in predId:
                            allFeatures[64]=1
                        else:
                            allFeatures[64] = 1
                        allFeatures[65]=numberTables
                        outf = ""
                        for k in sorted(list(allFeatures.keys())):
                            outf += str(k) + ":" + str(allFeatures.get(k)) + "\t"
                        out += outf + cluster + "\t" + t.tableId + "\t" + t.pos + "\t" + cell1.colName + "\t" + cell2.colName + "\t" + t.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                               t.obj.toString() + "\n"
                        #fout.write(
                        #    outf + cluster + "\t" + t.tableId + "\t" + t.pos + "\t" + cell1.colName + "\t" + cell2.colName + "\t" + t.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                         #   t.obj.toString() + "\n")

                    for predId in clusterPreds:
                        pred = wikidataDAO.getWikidataProp(predId)
                        if pred == None:
                            pred = PropertyStat(predId, predId)
                        getColPredicateTableFeatures(pred, relations, dictFeatures)
                        getColNameFeatures(pred.propName, cell1.colName, cell2.colName, dictFeatures)
                        getPredicateFeatures(pred, dictFeatures)
                        propName = pred.propName
                        domain = str(wikidataDAO.isInDomain(t.subj.id, pred.propId))
                        rangeC = str(wikidataDAO.isInRange(t.obj.id, pred.propId))
                        if "protag_article" in cell1.colName or 'protag_article' in cell2.colName:
                            dictFeatures[37] = 1
                        dictFeatures[38] = 2
                        dictFeatures[39] = domain
                        dictFeatures[40] = rangeC
                        featuresCluster = dictClusterFeaturesByCols.get(cols).get(pred.propId)
                        allFeatures = dictFeatures.copy()
                        allFeatures.update(featuresCluster)
                        objectsBySubj = wikidataDAO.getObjBySubjProp(t.subj.id, pred.propId)
                        allFeatures[49] = str(objectsBySubj)
                        subjByObj = wikidataDAO.getSubjByObjProp(t.obj.id, pred.propId)
                        allFeatures[50] = str(subjByObj)

                        allFeatures[60] = totalRows
                        allFeatures[61] = allFeatures[58] / allFeatures[56]
                        allFeatures[62] = allFeatures[59] / totalRows
                        allFeatures[63] = allFeatures[57] / allFeatures[55]
                        if "-1" in predId:
                            allFeatures[64]=1
                        else:
                            allFeatures[64] = 1
                        allFeatures[65]=numberTables
                        outf=""
                        for k in sorted(list(allFeatures.keys())):
                            outf += str(k) + ":" + str(allFeatures.get(k)) + "\t"

                        out += outf + cluster + "\t" + t.tableId + "\t" + t.pos + "\t" + cell1.colName + "\t" + cell2.colName + "\t" + t.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                                   t.obj.toString() + "\n"
                        #fout.write(
                        #    outf + cluster + "\t" + t.tableId + "\t" + t.pos + "\t" + cell1.colName + "\t" + cell2.colName + "\t" + t.subj.toString() + "\t" + pred.propId + " :" + pred.propName + "\t" + \
                        #    t.obj.toString() + "\n")
        del relationsByTables[table]
    #return out #+"RESULT"+cluster+"\t"+str(dictClusterPropertyByCols)+"\n"
    del relationsByTables
    del featuresTable
    del predsByTables
    del dictClusterFeaturesByCols
    del dictClusterPropertyByCols
    #gc.collect()
    return out

def process(input=0):
    # for each document that we want to process,
    #files = os.listdir(path)
    cluster =""
    listClusters=[]
    clustert=[]
    cont=0
    with gzip.open(FILE_OUTPUT, "wt") as fout:
        with gzip.open(FILE_CLUSTER, "rt") as fi:
            for line in fi:
                print("Line cluster: ", cont)
                if cont==0:
                    cont+=1
                    continue
                cont+=1
                _line=line.replace("\n","").split("\t")

                if int(_line[0])>=3000: # and int(_line[0])<=600000:
                    if cluster!=_line[0] and cluster!='':
                        #getTriplesCluster(clustert, fout)

                        copyCluster=clustert[:]
                        listClusters.append(copyCluster)
                        clustert=[_line[0]+"#"+_line[1]]
                        cluster=_line[0]
                    else:
                        cluster = _line[0]
                        clustert.append(_line[0]+"#"+_line[1])
            if len(clustert)>0:
                copyCluster = clustert[:]
                listClusters.append(copyCluster)
                #getTriplesCluster(clustert, fout)


    print("Total custers:", len(listClusters))
    for cluster in listClusters:
        print('len cluster', len(cluster))
        yield cluster

    # This will shutdown the entire pipeline once everything is done.
    yield Pipey.STOP


def processClusters(tables):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    #print("File: ", tableId)
    result = getTriplesCluster(tables)
    yield result

if __name__ == '__main__':

    args = sys.argv[1:]

    params = ConfigProperties().loadProperties()
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJson"  # params.get("json_files")
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillData()
    #wikidataDAO.fillDomainRange()
    #wikidataDAO.fillSubjObjCount()
    textProcessing = TextProcessing()
    FILE_CLUSTER=args[0]
    FILE_OUTPUT=args[1]
    #fileClusterRelations=args[2]
    #process()
    pipeline = Pipey.Pipeline()
    pipeline.add(process)
    pipeline.add(processClusters, 4)
    pipeline.add(ResultCombiner(FILE_OUTPUT))
    pipeline.run(100)
