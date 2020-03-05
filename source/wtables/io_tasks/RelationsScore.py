from wtables.preprocessing.TextProcessing import TextProcessing
import sys
import json
import pandas as pd
import numpy as np
import traceback
import gzip
def getAllRelations(fileIn, fileOut):
    cont = 0
    fout = gzip.open(fileOut, "wt")
    textProcessing=TextProcessing()
    with gzip.open(fileIn, "rt") as fin:
        for line in fin:
            print("Cont: ", cont)
            cont += 1
            line = line.replace("\n", "").strip()
            fields = line.split("\t")
            table = fields[0]
            rels = eval(fields[1])
            for k, v in rels.items():
                relsList = set()
                for k1, v1 in v.items():
                    if "#" in k1:
                        continue
                    else:
                        relsList.add(k1.replace("[", "").replace("]", ""))
                fout.write(table + "\t" + k +"\t" + " ".join(relsList) + "\n")
    fout.close()



def getMaxScoreRelation(file, fileOut):
    fout = gzip.open(fileOut, "wt")
    cont = 0
    with gzip.open(file, "rt") as fin:
        for line in fin:
            print("Line", cont)
            cont += 1
            _line = line.replace("\n", "").split("\t")
            dictt = eval(_line[1])
            #print(dictt)
            for k, v in dictt.items():
                drel = {}
                if len(v) == 0:
                    fout.write(_line[0] + "\t" + k + "\t" + "" + "\t" + "" + "\n")
                    continue
                for k1, v1 in v.items():
                    if "#" not in k1:
                        drel[k1] = float(v1)
                if len(drel) > 0:
                    drel = sorted(drel.items(), key=lambda kv: kv[1])
                    drel = drel[len(drel) - 1]
                    fout.write(_line[0] + "\t" + k + "\t" + drel[0] + "\t" + str(drel[1]) + "\n")
    fout.close()


def getMaxProtagonistScoreRelation(file, fileOut):
    fout = open(fileOut, "w")
    cont = 0
    with open(file, "r") as fin:
        for line in fin:
            print("Line", cont)
            cont += 1
            _line = line.replace("\n", "").split("\t")
            dictt = eval(_line[2])
            for k, v in dictt.items():
                if "protag_articl" in k:
                    drel = {}
                    for k1, v1 in v.items():
                        if "#" not in k1:
                            drel[k1] = float(v1)
                    if len(drel) > 0:
                        drel = sorted(drel.items(), key=lambda kv: kv[1])
                        drel = drel[len(drel) - 1]
                        fout.write(_line[0] + "\t" + k + "\t" + drel[0] + "\t" + str(drel[1]) + "\n")
    fout.close()


def getRelationsByTable(file, fileOut):
    cont = 0
    relationsByTable = {}
    fout = open(fileOut, "w")
    tableAux = ""
    with open(file, "r") as fin:
        for line in fin:
            print("Cont: ", cont)
            cont += 1
            line = line.replace("\n", "").strip()
            fields = line.split("\t")
            table = fields[0]
            headers = fields[1]
            headers = headers.replace("#", "no.")
            if len(fields) < 9:
                continue
            if fields[5].split("@")[1] != "3" or fields[6].split("@")[1] != "3":
                continue

            c1 = fields[5].split("@")[0]
            c2 = fields[6].split("@")[0]
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
            relations = fields[9].replace("[", "").replace("]", "").split(",")
            relations.sort()
            tkey = table + "##" + headers + "##" + c1 + "#" + c2
            if tableAux != table:
                if tableAux == "":
                    tableAux = table
                    relationsByTable[tkey] = relations[:]
                    continue
                for k, v in relationsByTable.items():
                    sv = set(v)
                    sv = list(sv)
                    sv.sort()
                    fout.write(k.replace("##", "\t") + "\t" + str(sv) + "\n")
                relationsByTable = {}
                relationsByTable[tkey] = relations[:]
                tableAux = table
            else:
                if relationsByTable.get(tkey) == None:
                    relationsByTable[tkey] = relations
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
        #print("read rel by table: ", count1, len(v))
        count1 += 1
        for i in range(len(v)):
            rv = v[i][:]
            for x in range(len(rv)):
                rel = "[" + rv[x] + "]"
                if relationsStats[k].get(rel) == None:
                    relationsStats[k][rel] = 1
                else:
                    relationsStats[k][rel] += 1

    for k, v in relationsStats.items():
        splitk = k.split("##")
        # print(k)
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
    tableAux = ""
    dictTable = {}
    table = ""
    headers = ""
    textProcessing=TextProcessing()
    with open(file, "r") as fin:
        for line in fin:
            print("Cont: ", cont)
            cont += 1
            try:
                line = line.replace("\n", "").strip()
                fields = line.split("\t")
                table = fields[0]
                if len(fields) <= 10:
                    continue

                pos=fields[4].split(":")
                col1=pos[2]
                col2=pos[3]
                headers=eval(fields[1].replace('\"\"', ''))
                headers =[textProcessing.cleanCellHeader(h) for h in headers]#textProcessing.cleanTableHeader(headers)
                col1n=int(col1)+1
                col2n=int(col2)+1
                c1=headers[col1n]
                c2=headers[col2n]
                setpair = set(list([c1, c2]))
                if fields[7] == fields[8]:
                    continue
                if len(fields) == 11 or fields[11]==None or fields[11]=="":
                    relations = []
                else:
                    relations = fields[11].replace("[", "").replace("]", "").split(",")
                    relations.sort()

                nrows = fields[3]
                tkey = table + "##" + str(headers) + "##" + c1 + "##" + c2 + "##" + nrows
                if tableAux != table:
                    if tableAux == "":
                        tableAux = table
                        relationsByTable = {}
                        relationsByTable[tkey] = [relations]
                        continue
                    fillRelations(dictTable, relationsByTable, table, headers, fout)
                    if dictTable.get(table + "##" + str(headers)) == None:
                        for k, v in dictTable.items():
                            fout.write(k.replace("##", "\t") + "\t" + str(v) + "\n")
                        dictTable = {}
                    tableAux = table
                    relationsByTable = {}
                    relationsByTable[tkey] = [relations]
                else:
                    if relationsByTable.get(tkey) == None:
                        relationsByTable[tkey] = [relations]
                    else:
                        relationsByTable[tkey].append(relations)
            except:
                traceback.print_exc()
                print('Error line: ', cont)
    fillRelations(dictTable, relationsByTable, table, headers, fout)
    for k, v in dictTable.items():
        fout.write(k.replace("##", "\t") + "\t" + str(v) + "\n")
    fout.close()




if __name__ == '__main__':
    args = sys.argv[1:]

    option= args[0]#"1"#
    if option=="1":
        getScoreRelations(args[1], args[2]) #("test.out", "test2.csv")##(args[1], args[2])#
    if option == "2":
        getMaxScoreRelation(args[1], args[2])#("test.out", "test2.csv") #(args[1], args[2])
    if option == "3":
        getAllRelations(args[1], args[2]) #
