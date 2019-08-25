from wtables.preprocessing import ReadHTML as readHTML
from wtables.schema.Article import *
from wtables.schema.Resource import Resource
from wtables.wikidata_db.WikidataDAO import *

def readFile(fileName):
    tableId = fileName.split("/")[len(fileName.split("/")) - 1].replace(".json", "").replace("_", ".")
    file = open(fileName, "r")
    obj = file.read()
    obj = json.loads(obj)
    # Converting json to Table object
    table = ComplexDecoderTable().default(obj)
    return table

def extractArticleResource(articleTitle):
    #Convert article of title to Link and get Wikidata ID
    _link = wikiLink(articleTitle)
    print("article:", _link)
    if _link != None and _link != "":
        wd= wikidataDAO.getWikidataID(_link)
        print("id a:", wd)
        if wd!="" and wd!=None:
            resource=Resource(_link)
            resource.setId(wd)
            return resource


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

    resources=list(resources.values())
    return resources

def createEntityMatrix(file):
    table=readFile(file)
    if table is None or table.htmlMatrix is None:
        return None
    #if table.htmlMatrix is None:
    #    return None
    htmlMatrix=np.array(table.htmlMatrix)

    entityMatrix=htmlMatrix.copy()
    entityMatrix.fill('')
    tablem =[]
    #Fill table headers:
    for row in range(0, table.startRows):
        rowm=[]
        for col in range(htmlMatrix.shape[1]):
            #entityMatrix[row][col]=htmlMatrix[row][col]
            rowm.append(readHTML.getTableCellText(htmlMatrix[row][col]))
        tablem.append(rowm)
    #Fill table content:

    for row in range(table.startRows, htmlMatrix.shape[0]):
        rowm=[]
        for col in range(htmlMatrix.shape[1]):
            resources=extractCellResources(htmlMatrix[row][col])
            entities=set()
            for res in resources:
                entities.add('wd::'+res.id)
            rowm.append(list(entities)) #json.dumps(entity.reprJSON(), cls=EntityComplexEncoder, skipkeys=True)
        tablem.append(rowm)
    table.htmlMatrix=tablem
    articleEntity=extractArticleResource(table.articleTitle)
    if articleEntity is not None:
        table.setArticleEntity(extractArticleResource(table.articleTitle).id)


    table.setTableType(table.tableType.value)
    ft = open(os.path.join(FOLDER_TABLES_OUT, str(table.tableId.replace(".", "_")) + ".json"), "w")
    ft.write(json.dumps(table.reprJSON(), cls=ComplexEncoder, skipkeys=True))
    ft.close()


def extractArticleClass(file):
    table=readFile(file)
    if table is None or table.htmlMatrix is None:
        return None
    #if table.htmlMatrix is None:
    #    return None
    entity=table.articleEntity
    entity_class = ''
    if entity is not None:
        entity_class=wikidataDAO.getMaxClass(entity)
        if entity_class is None:
            entity_class=''
    else:
        entity=''
    return table.tableId+"\t"+table.articleTitle+"\t"+entity+"\t"+entity_class+"\n"


def readDocuments(input=0):
    path = FOLDER_JSON_FILES
    files = os.listdir(path)
    for file in files:
        name, file_extension = os.path.splitext(file)
        if file_extension == ".json":
            yield path + "/" + file
    yield Pipey.STOP


def processDocuments(file):
    # perform some intensive processing on the document
    # note you can yield more than one result to the next stage
    print("File: ", file)
    return extractArticleClass(file)


if __name__ == '__main__':
    args = sys.argv[1:]
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJsonEntities"  # params.get("json_files")
    #FOLDER_TABLES_OUT="/home/jluzuria/tablesJsonEntities"
    params = ConfigProperties().loadProperties()
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillEntityMaxClass()

    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)

    pipeline.add(ResultCombiner('tablesArticleClass.csv.gz'))
    # One process combines the results into a file.
    pipeline.run(100)

