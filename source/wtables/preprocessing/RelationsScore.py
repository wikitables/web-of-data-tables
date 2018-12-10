import wtables.preprocessing.TextProcessing as textProcessing
import sys
import json

def getMaxScoreRelation(file, fileOut):
    fout=open(fileOut,"w")
    cont=0
    with open (file, "r") as fin:
        for line in fin:
            print("Line", cont)
            cont+=1
            _line=line.replace("\n","").split("\t")
            dictt=eval(_line[2])
            for k, v in dictt.items():
                drel = {}
                for k1, v1 in v.items():
                    if "#" not in k1:
                        drel[k1] = float(v1)
                if len(drel) > 0:
                    drel = sorted(drel.items(), key=lambda kv: kv[1])
                    drel = drel[len(drel) - 1]
                    fout.write(_line[0]+"\t"+k+"\t"+drel[0]+"\t"+str(drel[1])+"\n")
    fout.close()

def getRelationsByTable(file, fileOut):
    cont = 0
    relationsByTable = {}
    fout = open(fileOut, "w")
    tableAux = ""
    dictTable = {}
    with open(file, "r") as fin:
        for line in fin:
            print("Cont: ", cont)
            cont += 1
            line = line.replace("\n", "").strip()
            fields = line.split("\t")
            table = fields[0]
            headers = fields[1]
            if eval(headers)>100:
                continue
            if len(fields) < 11:
                continue
            if fields[6].split("@")[1] != "3" or fields[7].split("@")[1] != "3":
                continue

            c1 = fields[6].split("@")[0]
            c2 = fields[7].split("@")[0]
            if c1 == "spancol" or c2 == "spancol":
                continue
            c1 = textProcessing.removeStopWords(c1)
            c1 = textProcessing.removeSpecialCharacters(c1)
            c1 = textProcessing.stemWord(c1)
            c1 = c1.replace(" ", "_") + "@3"

            c2 = textProcessing.removeStopWords(c2)
            c2 = textProcessing.removeSpecialCharacters(c2)
            c2 = textProcessing.stemWord(c2)
            c2 = c2.replace(" ", "_") + "@3"
            setpair = set(list([c1, c2]))
            if len(setpair) == 1:
                continue
            relations = fields[10].replace("[", "").replace("]", "").split(",")
            relations.sort()
            tkey = table + "##" + headers + "##" + c1 + "#" + c2
            if tableAux != table:
                if tableAux == "":
                    tableAux = table
                    relationsByTable[tkey]=relations[:]
                    continue
                for k, v in relationsByTable.items():
                    sv=set(v)
                    sv=list(sv)
                    sv.sort()
                    fout.write(k.replace("##","\t")+"\t"+str(sv)+"\n")
                relationsByTable={}
                relationsByTable[tkey] = relations[:]
                tableAux = table
            else:
                if relationsByTable.get(tkey)==None:
                    relationsByTable[tkey]=relations
                else:
                    relationsByTable[tkey].extend(relations)
    for k, v in relationsByTable.items():
        sv = set(v)
        sv = list(sv)
        sv.sort()
        fout.write(k.replace("##", "\t") + "\t" + str(sv) + "\n")
    fout.close()

def fillRelations(dictTable, relationsByTable, table, headers, fout):
    relationsStats = {}
    # relationsStats[keyAux]={}
    count1 = 0
    for k, v in relationsByTable.items():
        relationsStats[k] = {}
        print("read rel by table: ", count1, len(v))
        count1 += 1
        for i in range(len(v)):
            rv = v[i][:]
            for x in range(len(rv)):
                rel = "[" + rv[x] + "]"
                if relationsStats[k].get(rel) == None:
                    relationsStats[k][rel] = 1
                else:
                    relationsStats[k][rel] += 1
                for y in range(x + 1, len(rv)):
                    rel = "[" + rv[x] + "#" + rv[y] + "]"
                    if relationsStats[k].get(rel) == None:
                        relationsStats[k][rel] = 1
                    else:
                        relationsStats[k][rel] += 1

            for j in range(i + 1, len(v)):
                rel1 = v[i]
                rel2 = v[j]
                for m in rel1:
                    for n in rel2:
                        pair = [m, n]
                        pair.sort()
                        pair = "[" + pair[0] + "#" + pair[1] + "]"
                        if relationsStats[k].get(pair) == None and m != n:
                            relationsStats[k][pair] = 0
    for k, v in relationsStats.items():
        splitk = k.split("##")
        nrows = float(splitk[4])
        for k1, v1 in v.items():
            if nrows == 0.0:
                nrows = 1
            ratio = float(v1) / nrows
            relationsStats[k][k1] = "%.3f" % round(ratio, 3)
    for k, v in relationsStats.items():
        ksplit = k.split("##")
        if dictTable.get(ksplit[0] + "##" + ksplit[1]) == None:
            dictTable[ksplit[0] + "##" + ksplit[1]] = {ksplit[2] + "#" + ksplit[3]: v}
        else:
            dictTable[ksplit[0] + "##" + ksplit[1]][ksplit[2] + "#" + ksplit[3]] = v

def getScoreRelations(file, fileOut):

    cont = 0
    relationsByTable = {}
    fout = open(fileOut, "w")
    tableAux=""
    dictTable = {}
    table = ""
    headers = ""
    with open(file, "r") as fin:
        for line in fin:
            print("Cont: ", cont)
            cont += 1
            line = line.replace("\n", "").strip()
            fields = line.split("\t")
            table = fields[0]
            headers = fields[1]
            if len(fields) < 11:
                continue
            if fields[6].split("@")[1] != "3" or fields[7].split("@")[1] != "3":
                continue

            c1 = fields[6].split("@")[0]
            c2 = fields[7].split("@")[0]
            if c1 == "spancol" or c2 == "spancol":
                continue
            c1 = textProcessing.removeStopWords(c1)
            c1 = textProcessing.removeSpecialCharacters(c1)
            c1 = textProcessing.stemWord(c1)
            c1 = c1.replace(" ", "_") + "@3"

            c2 = textProcessing.removeStopWords(c2)
            c2 = textProcessing.removeSpecialCharacters(c2)
            c2 = textProcessing.stemWord(c2)
            c2 = c2.replace(" ", "_") + "@3"
            setpair = set(list([c1, c2]))
            if len(setpair) == 1:
                continue

            relations = fields[10].replace("[", "").replace("]", "").split(",")
            relations.sort()
            nrows = fields[4]
            tkey = table + "##" + headers + "##" + c1 + "##" + c2 + "##" + nrows
            if tableAux!=table:
                if tableAux=="":
                    tableAux=table
                    relationsByTable={}
                    relationsByTable[tkey] = [relations]
                    continue
                fillRelations(dictTable,relationsByTable,table,headers,fout)
                if dictTable.get(table + "##" + headers) == None:
                    for k, v in dictTable.items():
                        fout.write(k.replace("##", "\t") + "\t" + str(v) + "\n")
                    dictTable = {}
                tableAux = table
                relationsByTable={}
                relationsByTable[tkey] = [relations]
            else:
                if relationsByTable.get(tkey)==None:
                    relationsByTable[tkey]=[relations]
                else:
                    relationsByTable[tkey].append(relations)
    fillRelations(dictTable, relationsByTable, table, headers, fout)
    for k, v in dictTable.items():
        fout.write(k.replace("##", "\t") + "\t" + str(v) + "\n")

    fout.close()


def evaluateRelations(propDictFile, pairPropFile,tables):
    fdict = open(propDictFile, "r")
    dictProp = {}
    for line in fdict.readlines():
        _line = line.replace("\n", "").split("\t")
        dictProp[_line[0]] = _line[1]

    f1 = open(pairPropFile, "r")
    dictPair = {}
    dictPairMax = {}
    for line in f1.readlines():
        _line = line.replace("\n","").split("\t")
        #print(_line)
        if _line[2]=="null" or _line[3]=="null":
            continue
        cont = float(_line[4])
        # val=max([(cont / float(_line[1])),(cont / float(_line[2]))])
        # dictPair[_line[0]]=val
        dictPairMax["["+_line[0]+"#"+_line[1]+"]"] = str(cont)+"\t" +_line[2]+"\t"+_line[3]

    f1.close()
    fout = open("evalPropertiesInvert.csv", "w")
    with open(tables, "r") as fileTables:
        for line in fileTables:
            _line = line.replace("\n", "").replace("\'", "\"").split("\t")
            h = _line[1].replace("\"", "").replace(" ", "")
            # print(h)
            # if h=="[rank@1,heat@1,name@3,nationality@3,time@2,notes@3]":
            dictScores = {}
            try:
                dictScores = json.loads(_line[2], encoding="utf-8")
            except:
                pass
                # print("Error line : ", line)

            for k, v in dictScores.items():
                prop = list(v.keys())

                prop.sort()
                ksplit=k.split("#")
                if ksplit[0].split("@")[0]=="" or ksplit[1].split("@")[0]=="":
                    #print("skipping :", k)
                    continue

                for pair in prop:

                    if "#" in pair:
                            #print(v)

                            _pair = pair.replace("[", "").replace("]", "").split("#")
                            #print(_pair)
                            # valp=dictPair.get(pair)
                            if dictPairMax.get(pair) != None:
                                probPair = float(v.get(pair))
                                prob1=float(v.get("["+_pair[0]+"]"))
                                prob2 = float(v.get("["+_pair[1]+"]"))
                                """pj = float(v.get(prop[j]))
                                if pi > 1:
                                    pi = 1.0
                                if pj > 1:
                                    pj = 1.0
                                scorePair = v.get("[" + pair + "]")
                                if scorePair != None:
                                    scorePair = str(scorePair)
                                else:
                                    scorePair = """
                                valPropi=_pair[0].split("-1")[0]
                                valPropj=_pair[1].split("-1")[0]
                                fout.write(_line[0]+"\t"+str(h)+"\t"+str(k) + "\t" + _pair[0] + "#" + dictProp.get(valPropi) + "\t" + _pair[1] + "#" + dictProp.get(valPropj) + "\t" + dictPairMax.get(pair) + "\t" + str(
                                    prob1) + "\t" + str(prob2) + "\t" + str(probPair) + "\n")


    fout.close()

if __name__ == '__main__':
    # fileName="/media/jhomara/Datos/MG-DCC/tesis/Desarrollo/datos/prueba/files1/109332.json"
    # inf=extractLinksFromColumns(fileName)
    # print(inf)
    args = sys.argv[1:]
    getScoreRelations(args[0],args[1])
    #getRelationsByTable(args[0],args[1])
    #getMaxScoreRelation("relationsByTables.csv", "maxRelTables.csv")
    #f0 = "/home/jhomara/Desktop/web7/propertiesVirtuoso.out"
    #f1 = "/home/jhomara/Desktop/web7/conflictRelations.csv"
    #f2 = "/home/jhomara/Desktop/web7/relationsByTables.out"
    #evaluateRelations(f0, f1, f2)

