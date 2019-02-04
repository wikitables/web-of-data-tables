# -*- coding: utf-8 -*-
import pandas as pd

from wtables.wikidata_db.ConfigProperties import ConfigProperties
from wtables.wikidata_db.WikidataDAO import *
import sys
import time
from wtables.preprocessing.TextProcessing import TextProcessing
from nltk.stem.snowball import SnowballStemmer
import gzip
import logging

def generateTriples(file):
    """Generate file triples with relations by cluster.
       :param filecluster: csv, tab separated [cluster, table, cols, relations, all_relations]
       :param fileLinks: csv, tab separated [table, headers, num_cols, num_rows, cell(ex: row:col1:col2),
                        name_col1, name_col2, link1, link2, entity1, entity2, relations(ex:[P1])]
       :param fileOut: csv, tab separated table triples format. Output example:
       447772.3	447772.3	2:1:3	titl@3	produc@3	https://en.wikipedia.org/wiki/By_Chance :Q25205603	P361 :part of@en	https://en.wikipedia.org/wiki/SremmLife_2 :Q23838424	1
    """
    out=""
    cont=0
    contSpanTriples=0
    contDescTriples = 0
    contNoteTriples = 0
    with gzip.open(FILE_OUT, 'wt') as fout:
        with gzip.open(file, "rt") as fin:
            for line in fin:
                print("Line: ", cont)
                _line = line.replace("\n", "").split("\t")
                wd1 = _line[5]
                wd2 = _line[6]
                if wd1 == "" or wd2 == "" or wd1 == wd2:
                    continue

                table = _line[0]
                cluster = dictTableCluster.get(table)
                if cluster == None:
                    continue
                row_col = _line[1]
                c1 = _line[2].split("##")[0]
                c2 = _line[2].split("##")[1]
                if "descrip" in c1 or "descrip" in c2:
                    contDescTriples+=1
                if "spancol" in c1 or "spancol" in c2:
                    contSpanTriples+=1
                if "note" in c1 or "note" in c2:
                    contNoteTriples+=1
                link1 = _line[3]
                link2 = _line[4]  # print(wd1, wd2)
                relations = _line[7]  # self.wikidataDAO.getRelations(wd1, wd2)

                if relations == None or relations == "":
                    relations = []
                else:
                    relations = relations.replace("[", "").replace("]", "").split(",")
                    relations = set([r.strip() for r in relations])
                allrels = dictCluster.get(cluster).get(c1 + "##" + c2)
                if allrels != None:
                    if len(relations) > 0:
                        newRelations = set(allrels) - set(relations)
                    else:
                        newRelations = allrels
                else:
                    #logging.debug("Pair columns not found.."+ cluster +" "+table +" "+c1 +" " +c2)
                    continue
                for rel in relations:
                    if rel == "":
                        continue
                    prop = wikidataDAO.getWikidataProp(rel.strip())
                    # print(rel, prop.propId, prop.propName)
                    # print("relations", relations)
                    if prop == None:
                        #logging.debug("None prop: " + rel)
                        propId = rel
                        propName = rel
                    else:
                        propId = prop.propId
                        propName = prop.propName
                    #existentTriples += 1
                    fout.write(str(
                        cluster) + "\t" + table + "\t" + row_col + "\t" + c1 + "\t" + c2 + "\t" + link1 + " :" + wd1 + "\t" + propId + " :" + propName + "\t" + link2 + " :" + wd2 + "\t" + "1" + "\n")
                    #prop = wikidataDAO.getWikidataProp(rel)
                for rel in newRelations:
                    if rel == "":
                        continue
                    prop = wikidataDAO.getWikidataProp(rel)
                    if prop == None:
                        #loggin.debug("None prop: "+rel)
                        propId = rel
                        propName = rel
                    else:
                        propId = prop.propId
                        propName = prop.propName
                    #noExistentTriples += 1
                    fout.write(str(
                        cluster) + "\t" + table + "\t" + row_col + "\t" + c1 + "\t" + c2 + "\t" + link1 + " :" + wd1 + "\t" + propId + " :" + propName + "\t" + link2 + " :" + wd2 + "\t" + "0" + "\n")




def generateTriples1(file):
    cont=0
    with gzip.open(FILE_OUT, 'wt') as fout:
        with gzip.open(file, "rt") as fin:
            for line in fin:
                print("Line: ", cont)
                cont+=1
                _line = line.replace("\n", "").split("\t")
                #print(_line)
                if len(_line)<13:
                    continue
                wd1 = _line[9]
                wd2 = _line[10]
                if wd1 == "" or wd2 == "" or wd1 == wd2:
                    continue

                table = _line[0]
                cluster = dictTableCluster.get(table)
                if cluster == None:
                    continue
                print("cluster", cluster)
                row_col = _line[4]
                c1 = _line[5]
                c2 = _line[6]
                link1 = _line[7]
                link2 = _line[8]  # print(wd1, wd2)
                relations = _line[12]  # self.wikidataDAO.getRelations(wd1, wd2)
                print("Pair:", c1+"##"+c2)
                if relations == None or relations == "":
                    relations = []
                else:
                    relations = eval(relations) #.replace("[", "").replace("]", "").split(",")
                    relations = set([r.strip() for r in relations])
                allrels = dictCluster.get(cluster).get(c1 + "##" + c2)
                if allrels != None:
                    if len(relations) > 0:
                        newRelations = set(allrels) - set(relations)
                    else:
                        newRelations = allrels
                else:
                    print("Pair columns not found.."+ cluster +" "+table +" "+c1 +" " +c2)
                    continue

                for rel in relations:
                    if rel == "":
                        continue
                    prop = wikidataDAO.getWikidataProp(rel.strip())
                    # print(rel, prop.propId, prop.propName)
                    # print("relations", relations)
                    if prop == None:
                        print("None prop: " + rel)
                        propId = rel
                        propName = rel
                    else:
                        propId = prop.propId
                        propName = prop.propName
                    #existentTriples += 1
                    fout.write( str(
                        cluster) + "\t" + table + "\t" + row_col + "\t" + c1 + "\t" + c2 + "\t" + link1 + " :" + wd1 + "\t" + propId + " :" + propName + "\t" + link2 + " :" + wd2 + "\t" + "1" + "\n")
                    #prop = wikidataDAO.getWikidataProp(rel)
                for rel in newRelations:
                    if rel == "":
                        continue
                    prop = wikidataDAO.getWikidataProp(rel)
                    if prop == None:
                        #loggin.debug("None prop: "+rel)
                        propId = rel
                        propName = rel
                    else:
                        propId = prop.propId
                        propName = prop.propName
                    #noExistentTriples += 1
                    fout.write(str(
                        cluster) + "\t" + table + "\t" + row_col + "\t" + c1 + "\t" + c2 + "\t" + link1 + " :" + wd1 + "\t" + propId + " :" + propName + "\t" + link2 + " :" + wd2 + "\t" + "0" + "\n")


def getClusterRelations(fileCluster):
    dictTableCluster = {}
    dictCluster = {}
    cont=0
    with open(fileCluster, "r") as fileCl:
        for line in fileCl:
            print("cont: ", cont)
            if cont == 0:
                cont += 1
                continue
            #if cont>1000:
            #    break
            cont += 1
            _line = line.replace("\n", "").split("\t")
            cluster = _line[0]
            table = _line[1]
            all_relations = _line[4]
            if all_relations == None or all_relations == "":
                continue
            else:
                all_relations = eval(all_relations)
                all_relations = {r.strip() for r in all_relations}
            cols = _line[2]
            dictTableCluster[table] = cluster
            if dictCluster.get(cluster) == None:
                dictCluster[cluster] = {cols: all_relations}
            else:
                if dictCluster.get(cluster).get(cols) == None:
                    dictCluster[cluster][cols] = all_relations
    return dictTableCluster, dictCluster


def readLinks(input=0):
    cont=0
    line_out=""
    fi =gzip.open(FILE_LINKS, "rt")
    lines=fi.readlines()
    for line in lines:
            if cont>=50000:
                outyield=line_out
                line_out=""
                cont = 1
                yield outyield
            else:
                line_out+=line
                cont+=1
    if line_out!="":
        yield line_out
    yield Pipey.STOP


def processLinks1(line):


    result = generateTriples1(line)
    if result!="":
        #print(result)
        yield result


if __name__ == '__main__':
    args = sys.argv[1:]
    logging.basicConfig(filename="./debug.log", level=logging.DEBUG)
    params = ConfigProperties().loadProperties()
    wikidataDAO = WikidataDAO(params)
    wikidataDAO.fillPropName()
    option=args[0]
    FILE_CLUSTER = args[1]#"cluster0.csv"
    FILE_LINKS = args[2]#"test2.csv.gz"
    FILE_OUT =args[3]#"out.out.gz"
    dictTableCluster, dictCluster = getClusterRelations(FILE_CLUSTER)
    startTime = time.time()
    if option=="1":
        generateTriples1(FILE_LINKS)
    if option=="2":
        generateTriples(FILE_LINKS)
    #pipeline = Pipey.Pipeline()
    # one process reads the documents
    #pipeline.add(readLinks)
    # up to 8 processes transform the documents
    #if args[0]=='1':
    #    pipeline.add(processLinks1, 8)
    #if args[0]=='2':
    #    pipeline.add(processLinks, 8)
    # One process combines the results into a file.
    #pipeline.add(ResultCombiner(FILE_OUT))
    #pipeline.run(50)
    print("Time Triples: ", time.time() - startTime)
