from nltk import ngrams
from nltk.stem import PorterStemmer
import re
import pandas as pd
import numpy as np
from multiprocessing import Manager, Array
import multiprocessing
from joblib import Parallel, delayed
import wtables.preprocessing.TextProcessing as textProcessing



def tokenizeVector(vector):
    hvector = vector[:]
    stemVector = []
    for text in hvector:
        if text != "spancol" and text != "":

            h = text.split("@")[0].split("_")
            stemText = [textProcessing.stemWord(w) for w in h]
            stemText = textProcessing.removeStopWords(stemText)
            stemmed = []
            for w in stemText:
                if w != None and w != "":
                    stemmed.append(w)
            if len(stemmed) > 0:
                stemVector.append("_".join(stemmed))

    stemVector = set(stemVector)
    stemVector = list(stemVector)
    stemVector.sort()
    return stemVector


def generateDict(inputFile):
    # Save header as vector of numbers
    dict_file = "dict.txt"
    dictf = open(dict_file, "w")
    dict = {}
    dict_count = {}
    count = 0
    with open(inputFile, "r", encoding="utf-8") as inFile:
        cont = 0
        for line in inFile:
            print("line: ", cont)
            cont += 1
            l = line.replace("\n", "").split("\t")
            if l[1]=="1":
                h = eval(l[0])[1]
                #h.extend(eval(l[0])[2])
                #print(h)
                vectorStemmed = tokenizeVector(h)
                for w in vectorStemmed:
                    sw = w.strip()
                    if dict.get(sw) == None and sw != "" and sw != "spancol":
                        dict[sw] = count
                        dict_count[sw] = 1
                        count += 1
                    else:
                        if dict.get(sw) != None and sw != "" and sw != "spancol":
                            dict_count[sw] += 1
    for k, v in dict.items():
        dictf.write(str(v) + "\t" + str(k) + "\t" + str(dict_count.get(k)) + "\n")
    dictf.close()


def vectorizeFilter(inputFile):
    # Save header as vector of numbers
    output_file = "vect.txt"

    dictf = getInverseDict()
    out = open(output_file, "w")

    with open(inputFile, "r", encoding="utf-8") as inFile:
        cont = 0
        for line in inFile:
            if cont==0:
                cont+=1
                continue
            print("line:", cont)
            cont += 1

            l = line.replace("\n", "").split("\t")
            larr=eval(l[0])
            number = larr[0]
            h = larr[1]
            #h.extend(larr[2])
            vectorStemmed = tokenizeVector(h)
            setvec = []
            for w in vectorStemmed:
                sw = w.strip()
                regex = r'\d+'
                listi = re.findall(regex, sw)
                if len(listi) > 0:
                    continue
                if dictf.get(sw) != None:
                    val = eval(dictf.get(sw))
                    if val[1] > 1:
                        setvec.append(val[0])
            if len(setvec) > 0:
                setvec.sort()
                out.write(str(number) + "\t" + str(setvec) + "\n")
    out.close()


def vectorize(inputFile):
    # Save header as vector of numbers
    output_file = "vect.txt"

    dictf = getInverseDict()
    out = open(output_file, "w")

    with open(inputFile, "r", encoding="utf-8") as inFile:
        cont = 0
        for line in inFile:
            print("line:", cont)
            cont += 1
            l = line.replace("\n", "").split("\t")
            number = l[0]
            l = l[1]
            vectorStemmed = tokenizeVector(l)
            setvec = []
            for w in vectorStemmed:
                sw = w.strip()
                if dictf.get(sw) != None:
                    val = eval(dictf.get(sw))
                    if val[1] > 1:
                        setvec.append(val[0])
            if len(setvec) > 0:
                setvec.sort()
                out.write(str(number) + "\t" + str(setvec) + "\n")
    out.close()


def getItemFreqDict():
    dict_file = "dict.csv"
    dictf = open(dict_file, "r")
    lines = dictf.readlines()
    dict_vocab = {}
    for line in lines:
        word = line.replace("\n", "").split("\t")
        dict_vocab[str(word[0])] = int(word[2])
    dictf.close()
    return dict_vocab


def generateGraph(nameVectFile):
    fo = open("clusters_pair.csv", "w")
    cont = 0
    dvect = {}
    listVect=[]
    with open(nameVectFile, "r") as vectFile:
        for line in vectFile:
            print("cont ", cont)
            sentence = line.replace("\n", "").split("\t")
            listVect.append(sentence)
            sentence = sentence[1]
            sentence = eval(sentence)
            for i in range(len(sentence)):
                for j in range(i + 1, len(sentence)):
                    if dvect.get(sentence[i]) == None:
                        dvect[sentence[i]] = {sentence[j]: 1}
                        dvect[sentence[j]] = {sentence[i]: 1}
                    else:
                        child = dvect[sentence[i]].get(sentence[j])
                        if child == None:
                            dvect[sentence[i]][sentence[j]] = 1
                        else:
                            dvect[sentence[i]][sentence[j]] = child + 1
                    if dvect.get(sentence[j]) == None:
                        dvect[sentence[j]] = {sentence[i]: 1}
                    else:
                        if dvect.get(sentence[j]).get(sentence[i]) == None:
                            dvect[sentence[j]][sentence[i]] = 1
                        else:
                            dvect[sentence[j]][sentence[i]] += 1
            cont += 1
    dictv = getItemFreqDict()
    total = len(dictv)
    newDict={}
    for k, v in dvect.items():
        for k1, v1 in v.items():

            f1 = v1 / total #dictv.get(str(k))
            #f2 = v1 / total #dictv.get(str(k1))
            #if f1 >= f2:
            newDict[str([k,k1])] = f1
            #fo.write(str(k) + "\t" + str(k1) + "\t" + str(f1) + "\n")
            #else:
            #    fo.write(str(k1) + "\t" + str(k) + "\t" + str(f2) + "\n")
                # fodict.write(str(dict_g)+"\n")

    listScoreSorted = sorted(newDict.items(), key=lambda kv: kv[1])
    dictCluster={}
    cluster=0
    rest=listVect[:]
    fin=False
    while(rest):
        listVect = rest[:]
        if len(listScoreSorted)==0:
            for v in listVect:
                dictCluster[str(v)] = cluster
                fin=True
                break
        if fin:
            break
        pair=listScoreSorted.pop(len(listScoreSorted)-1)
        rest.clear()
        for v in listVect:
            if set(eval(pair[0])).issubset(set(eval(v[1]))):
                dictCluster[str(v)]=cluster
            else:
                rest.append(v)
        cluster+=1
        print("len rest: ", len(rest))
    for k, v in dictCluster.items():
        fo.write(str(k)+"\t"+str(v)+"\n")
    fo.close()


def clusterPair(nameVectFile):
    cont = 0
    dvect = {}
    dvfile={}
    listVect=[]
    with open(nameVectFile, "r") as vectFile:
        for line in vectFile:
            print("cont ", cont)
            sentence = line.replace("\n", "").split("\t")
            listVect.append(sentence)
            table=sentence[0]
            sentence = sentence[1]
            sentence = eval(sentence)
            dvfile[table]=sentence
            for i in range(len(sentence)):
                for j in range(i + 1, len(sentence)):
                    op1=str([sentence[i], sentence[j]])
                    op2=str([sentence[j], sentence[i]])
                    if dvect.get(op1)==None and dvect.get(op2)==None:
                        dvect[op1]=[table]
                        dvect[op2] = [table]
                    else:
                        dvect[op1].append(table)
                        dvect[op2].append(table)
            cont+=1
    clusterd={}
    clusterh={}
    cluster=0
    fout=open("fileClusterPair.csv", "w")
    for k, v in dvect.items():
        for table in v:
            if clusterd.get(table)==None:
                elementsTable=dvfile.get(table)
                tempDict={}
                for k1, v1 in clusterh.items():
                    intersect=set(elementsTable).intersection(v1)
                    if len(intersect)>=2:
                        metric=len(intersect)/len(set(elementsTable).union(v1))
                        if metric>0:
                            tempDict[k1]=metric
                listScoreSorted = sorted(tempDict.items(), key=lambda kv: kv[1])
                if len(listScoreSorted)>0:
                    #print("listScore sorted", listScoreSorted)
                    #print(listScoreSorted[len(listScoreSorted)-1][0])
                    clusterd[table]=listScoreSorted[len(listScoreSorted)-1][0]
                else:
                    clusterd[table]=cluster
                if clusterh.get(cluster)==None:
                    clusterh[cluster]=set(dvfile.get(table))
                else:
                    clusterh[cluster].union(set(dvfile.get(table)))
        #print(clusterd)
        cluster+=1
    clusterh.clear()
    dictd=getDict()
    for k, v in clusterd.items():
        vect=dvfile.get(k)
        print(vect)
        print("vect 0", dictd.get(str(vect[0])))
        dv=[dictd.get(str(w)) for w in vect]
        fout.write(str(k)+"\t"+str(dv)+"\t"+str(v)+"\n")
    fout.close()


def loadEdges():
    dedges = {}
    with open("edges.csv", "r") as edFile:
        for line in edFile:
            l = line.replace("\n", "").split("\t")
            dedges[l[0] + "#" + l[1]] = float(l[2])
    return dedges


def vectorizePairs(vfile):
    # Save header as vector of numbers
    fo = open("vect_pair.txt", "w")

    print("Edges loaded")
    dvect = loadEdges()
    cont = 0
    with open(vfile, "r") as edFile:
        for line in edFile:
            print("Cont: ", cont)
            cont += 1
            l = line.replace("\n", "").split("\t")
            sentence = eval(l[1])
            acum = []
            for i in range(len(sentence)):
                for j in range(i + 1, len(sentence)):
                    pair1 = str(sentence[i]) + "#" + str(sentence[j])
                    pair2 = str(sentence[j]) + "#" + str(sentence[i])
                    val1 = dvect.get(pair1)
                    val2 = dvect.get(pair2)
                    if val1 != None:
                        acum.append(pair1 + "@" + str(val1))
                    if val2 != None:
                        acum.append(pair2 + "@" + str(val2))
            fo.write(l[0] + "\t" + str(acum) + "\n")
            acum.clear()
    fo.close()


def generateNgrams(ngram, nameVectFile):
    """
    :param ngram: quantity of headers to build the ngram
    :param nameVectFile: file with list of vectors generated in vectorize method
    :return:
    """

    # List of ngrams
    output_file = "ngram_list.txt"  # sys.argv[2]
    # Dictionary by vector
    # ngram_dict="ngram_dict.txt"
    fo = open(output_file, "w")
    # fodict=open(ngram_dict,"w")
    cont = 0
    with open(nameVectFile, "r") as vectFile:
        for line in vectFile:
            print("cont ", cont)
            sentence = line.replace("\n", "").split("\t")
            number = sentence[0]
            sentence = sentence[1]
            sentence = eval(sentence)
            grams = []
            for i in range(len(sentence)):
                for j in range(i + 1, len(sentence)):
                    gram = str(sentence[i]) + "_" + str(sentence[j])
                    grams.append(gram)
            if len(grams) > 0:
                fo.write(number + "\t" + str(grams) + "\n")
            cont += 1
            # fodict.write(str(dict_g)+"\n")
    fo.close()
    # fodict.close()


def fiterNgrams(nameNgramsFile, filter):
    """
    :param nameNgramsFile: File with list of ngrams generated by generateNgrams
    :param filter: True for deleting ngrams with one ocurrence, false in otherwise.
    :return:
    """

    # File with filter ngrams and number of ocurrences
    output_file = "ngram_list_filtered.txt"
    uniq_gram = {}
    more_one = {}
    cont = 0
    if filter:
        with open(nameNgramsFile, "r") as file:
            for line in file:
                l = line.replace("\n", "")
                print("cont:", cont)
                if more_one.get(l) == None:
                    if uniq_gram.get(l) == None:
                        uniq_gram[l] = 1
                    else:
                        more_one[l] = 2
                        uniq_gram.pop(l)
                else:
                    cont += 1
                    more_one[l] = more_one[l] + 1
    else:
        with open(nameNgramsFile, "r") as file:
            for line in file:
                l = line.replace("\n", "")
                print("cont:", cont)
                if more_one.get(l) == None:
                    more_one[l] = 1
                else:
                    more_one[l] = more_one[l] + 1
                cont += 1

    fo = open(output_file, "w")
    for k, v in more_one.items():
        fo.write(str(k) + "\t" + str(v) + "\n")
    fo.close()


def cluster_tables(gramsListFile, dictGramFile, tablesFile):
    """
    :param gramsListFile: List of filtered list of ngrams
    :param dictGramFile: Dictionary by vector generated by generateNgrams
    :param tablesFile: Main file with list of tables and headers.
    :return:
    """
    file_gram = open(gramsListFile, "r")
    lines = file_gram.readlines()
    dict_gram = {}
    gram_count = {}
    for i, line in enumerate(lines):
        word = line.replace("\n", "").split("\t")
        print("line:", i)
        dict_gram[str(word[0])] = []
        gram_count[str(word[0])] = int(word[1])
    lines.clear()
    file_gram.close()

    with open(dictGramFile, "r") as file:
        count = 0
        for line in file:
            print("count:", count)
            dict_line = eval(line)
            k_grams = dict_line.keys()

            # Getting the most frequent ngram by vector (table)
            # Each ngram most frequent will be a  "cluster".
            frequentg = max_gram(list(k_grams), gram_count)
            if frequentg != None:
                values = dict_line.values()
                value = list(values)[0]
                if dict_gram.get(str(frequentg)) != None:
                    dict_gram[str(frequentg)].append(value)
                count += 1

    # Getting tables by id_table
    tf = open(tablesFile, "r", encoding="utf-8")
    linestf = tf.readlines()
    dict_tables = {}
    for line in linestf:
        ls = line.split("\t")
        dict_tables[ls[3]] = line

    # Getting header values from dict.txt
    dict_vocab = getDict()

    # Generating output file adding the ngram cluster to each table on list.
    cluster_file = "cluster_file_c.csv"
    fo = open(cluster_file, "w", encoding="utf-8")
    for k, v in dict_gram.items():
        for idx in v:
            table = dict_tables[idx]
            ks = [dict_vocab.get(str(ki)) for ki in eval(k)]
            ks.sort()
            fo.write(str(ks) + "\t" + str(len(v)) + "\t" + str(idx) + "\t" + table)
    fo.close()


def max_gram(list_grams, pair_count):
    dict_gram = {}
    for g in list_grams:
        if pair_count.get(str(g)) != None:
            dict_gram[str(g)] = pair_count.get(str(g))
    if len(dict_gram.items()) > 0:
        sorted_by_value = sorted(dict_gram.items(), key=lambda kv: kv[1])
        v = sorted_by_value[len(sorted_by_value) - 1]
        v = v[0]
        return v


def getDict():
    dict_file = "dict.txt"
    dictf = open(dict_file, "r")
    lines = dictf.readlines()
    dict_vocab = {}
    for line in lines:
        word = line.replace("\n", "").split("\t")
        dict_vocab[str(word[0])] = word[1]
    dictf.close()
    return dict_vocab


def getInverseDict():
    dict_file = "dict.csv"
    dictf = open(dict_file, "r")
    lines = dictf.readlines()
    dict_vocab = {}
    for line in lines:
        word = line.replace("\n", "").split("\t")
        dict_vocab[word[1]] = "[" + str(word[0]) + "," + str(word[2]) + "]"
    dictf.close()
    return dict_vocab


def aglomerative(line, queue, cont):
    print("line: ", cont)
    split = line.replace("\n", "").split("\t")
    table = split[0]
    vect = eval(split[1])
    newVect = []
    for pair in vect:
        #print(split[1])
        #print(pair)
        splitPair = pair.split("#")
        p1 = splitPair[0]
        p2 = splitPair[1].split("@")[0]
        newVect.append(p1)
        newVect.append(p2)
        try:
            neighbors = EDGES.get_group(p2)
        except KeyError:
            continue
        neighbors = neighbors["target"].tolist()
        newVect.extend(neighbors)
    newVect=list(set(newVect))
    lenv=len(newVect)
    if lenv>0:
         queue.put(table + "\t" + str(newVect) + "\t"+str(lenv)+"\n")
    newVect.clear()

def Writer(dest_filename, some_queue, some_stop_token):
    with open(dest_filename, 'w') as dest_file:
        while True:
            line = some_queue.get()
            if line == some_stop_token:
                return
            dest_file.write(line)

"""
EDGES = pd.read_csv("edges.csv", sep="\t", dtype={"source": np.str_, "target": np.str_}, decimal=".", index_col=False)
EDGES = EDGES.groupby('source')
m = multiprocessing.Manager()
queue = m.Queue()
queue.put("table1" + "\t" + "table2" + "\t" + "score"+"\n")
fi=open("vect_pair.txt", "r")
lines=fi.readlines()
Parallel(n_jobs=4)(delayed(aglomerative)(line, queue, i)
                                   for i, line in enumerate(lines))
queue.put("STOP")
writer_process = multiprocessing.Process(target=Writer, args=("vect_extended.txt", queue, "STOP"))
writer_process.start()
writer_process.join()
"""
ifile = "cluster1.csv"
#generateDict(ifile)
#vectorizeFilter(ifile)
clusterPair("vect.txt")
#generateGraph("vect.txt")
#vectorizePairs("vect.txt")
#aglomerative("edges.csv")
# generateNgrams(2, "vect.txt")
# fiterNgrams("ngram_list.txt", False)
# cluster_tables("ngram_list_filtered.txt", "ngram_dict.txt", ifile)
