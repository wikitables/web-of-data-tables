from wtables.utils.ParseLink import *
import sys
def getColumnWikidataRelations(fileEntities, fileRelations, fileLinks):
    # fileResults = open(fileResults, "w")
    fileTableRelations = open("fileTableRelations.out", "w")
    dictEntities = {}
    with open(fileEntities, "r") as fileE:
        for line in fileE:
            splitLine = line.replace("\n", "").split("\t")
            entityLink = splitLine[1].split("/")
            entity = entityLink[len(entityLink) - 1]
            dictEntities[splitLine[0]] = entity

    dictRelations = {}
    with open(fileRelations, "r") as fileR:
        for line in fileR:
            splitLine = line.replace("\n", "").split("\t")
            try:
                relation = splitLine[0] + "#" + splitLine[2]
                if dictRelations.get(relation) == None:
                    dictRelations[relation] = [splitLine[1]]
                else:
                    dictRelations[relation].extend(splitLine[1])
                    dictRelations[relation] = list(set(dictRelations[relation]))
            except:
                print("Error fileRelations", str(splitLine))
    cont = 0
    with open(fileLinks, "r") as fileL:
        for line in fileL:
            print("Line: ", cont)
            cont += 1
            splitLine = line.replace("\n", "").split("\t")
            if len(splitLine) != 10:
                print("Error fileLinks", str(splitLine))
                continue
            col1 = wikiLink(splitLine[8])
            col2 = wikiLink(splitLine[9])
            relationsFound = False
            rel = []
            if col1 != "" and col2 != "":
                col1 = dictEntities.get(col1)
                col2 = dictEntities.get(col2)
                if col1 != None and col2 != None:
                    # dictTables[idTable][1] += 1
                    rel = dictRelations.get(col1 + "#" + col2)
                    rel2 = dictRelations.get(col2 + "#" + col1)
                    if rel != None and rel2 != None:
                        rel.extend(rel2)
                    if rel == None and rel2 != None:
                        rel = rel2
                    if rel != None and len(rel) > 0:
                        rel = list(set(rel))
                        if len(rel) == 1 and rel[0] != "about":
                            relationsFound = True
                            fileTableRelations.write(line.replace("\n", "") + "\t" + str(rel) + "\n")
            if rel!=None:
                rel.clear()
            if relationsFound == False:
                fileTableRelations.write(line.replace("\n", "") + "\t" + "" + "\n")
    fileTableRelations.close()

if __name__ == '__main__':
    # fileName="/media/jhomara/Datos/MG-DCC/tesis/Desarrollo/datos/prueba/files1/109332.json"
    # inf=extractLinksFromColumns(fileName)
    # print(inf)
    args = sys.argv[1:]
    getColumnWikidataRelations(args[0], args[1], args[2])