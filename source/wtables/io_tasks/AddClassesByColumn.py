import json
import sys
from wtables.schema.Article import *
from wtables.wikidata_db.WikidataDAO import *
import numpy as np
def readFile(fileName):
    tableId = fileName.split("/")[len(fileName.split("/")) - 1].replace(".json", "").replace("_", ".")
    file = open(fileName, "r")
    obj = file.read()
    obj = json.loads(obj)
    # Converting json to Table object
    table = ComplexDecoderTable().default(obj)
    return table

def extractEntities(file):
    """
    Read json table with entities and create a new table with classes replacing entities.
    :param file: json table

    """

    table=readFile(file)
    if table is None:
        return
    htmlMatrix=np.array(table.htmlMatrix, object)
    headers=table.colHeaders
    if len(headers)==0:
        return

    dictColClasses = {str(i)+"###"+h:{} for i, h in enumerate(headers)}
    coli=0
    for col in range(htmlMatrix.shape[1]):
        if coli>=len(headers):
            print('Column out of headers: ', table.tableId, coli)
            continue
        colname=str(coli)+"###"+headers[coli]
        for row in range(table.startRows,htmlMatrix.shape[0]):
            listEntities=htmlMatrix[row][col]

            if len(listEntities)==0:
                continue
            #print(listEntities)
            for entity in listEntities:
                entity=entity.replace("wd::","")
                #print("e: ", entity)
                entityClasses=wikidataDAO.getClasses(entity)
                if entityClasses is None:
                    continue
                for classe in entityClasses:

                    actualClass=dictColClasses.get(colname).get(classe)
                    if actualClass is None:
                        dictColClasses[colname][classe]=1
                    else:
                        dictColClasses[colname][classe]+=1
        coli+=1
    table.setColumnClasses(dictColClasses)
    table.setTableType(table.tableType.value)
    ft = open(os.path.join(FOLDER_TABLES_OUT, str(table.tableId.replace(".", "_")) + ".json"), "w")
    ft.write(json.dumps(table.reprJSON(), cls=ComplexEncoder, skipkeys=True))
    ft.close()

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
    return extractEntities(file)


if __name__ == '__main__':
    args = sys.argv[1:]
    FOLDER_JSON_FILES = "/home/jluzuria/tablesJsonEntities"  # params.get("json_files")
    FOLDER_TABLES_OUT="/home/jluzuria/tablesJsonEntityClasses"
    params = ConfigProperties().loadProperties()
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillDomainRange()

    pipeline = Pipey.Pipeline()
    # one process reads the documents
    pipeline.add(readDocuments)
    # up to 8 processes transform the documents
    pipeline.add(processDocuments, 8)
    # One process combines the results into a file.
    pipeline.run(100)
