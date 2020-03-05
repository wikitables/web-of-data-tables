import gzip
import sys


def extractRangeDomain(fileSubj, fileObj, fileEntityInstances):
    dictEntityClasses = {}
    line_count=0
    with gzip.open(fileEntityInstances, "rt") as fileIn:
        for line in fileIn:
            print("Line: ", line_count)
            line_count += 1
            split = line.replace("\n", "").split("\t")
            entity = split[0]
            classes = eval(split[1])
            dictEntityClasses[entity] = classes

    dictClasses = {}
    dictRange = {}
    oldEntity = ""
    line_count = 0
    with gzip.open(fileSubj, "rt") as fileIn:
        for line in fileIn:
            print("Line: ", line_count)
            line_count += 1
            split = line.replace("\n", "").split("\t")
            predicate = split[1]
            if predicate.strip() != "P31":
                classes = dictEntityClasses.get(split[0])
                if classes is not None:
                    classDomain = dictClasses.get(predicate)
                    if classDomain is None:
                        dictClasses[predicate]={}
                        for ed in classes:
                            dictClasses[predicate][ed]=1
                    else:
                        for ed in classes:
                            val=dictClasses[predicate].get(ed)
                            if val is None:
                                dictClasses[predicate][ed]=1
                            else:
                                dictClasses[predicate][ed]=val+1
    print("writing Domain")
    with gzip.open('predicateDomain.out.gz', "wt") as fileOut:
        for k, v in dictClasses.items():
            fileOut.write(k+"\t"+str(v)+"\n")
    dictClasses={}
    with gzip.open(fileObj, "rt") as fileIn:
        for line in fileIn:
            print("Line: ", line_count)
            line_count += 1
            split = line.replace("\n", "").split("\t")
            predicate = split[0]
            if predicate.strip() != "P31":
                classes = dictEntityClasses.get(split[1])
                if classes is not None:
                    classDomain = dictClasses.get(predicate)
                    if classDomain is None:
                        dictClasses[predicate] = {}
                        for ed in classes:
                            dictClasses[predicate][ed] = 1
                    else:
                        for ed in classes:
                            val = dictClasses[predicate].get(ed)
                            if val is None:
                                dictClasses[predicate][ed] = 1
                            else:
                                dictClasses[predicate][ed] = val + 1
    print("writing Range")
    with gzip.open('predicateRange.out.gz', "wt") as fileOut:
        for k, v in dictClasses.items():
            fileOut.write(k+"\t"+str(v)+"\n")

if __name__ == '__main__':
    args = sys.argv[1:]
    extractRangeDomain(args[0], args[1], args[2])
