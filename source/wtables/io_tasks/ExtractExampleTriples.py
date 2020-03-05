import gzip
import sys
def extractExampleTriples(filePredicates, fileTriples):
    dictPredicates={}
    setPredicates={}
    with open(filePredicates, "r") as fileIn:
        for line in fileIn:
            _line=line.replace("\n","").split("\t")
            dictPredicates[_line[0]]=""
            dictPredicates[_line[0]+"-1"] = ""
            setPredicates[_line[0]] = 0
            setPredicates[_line[0] + "-1"] = 0
    print('Len predicates: ', len(setPredicates))
    with gzip.open('exampleTriples.csv.gz', 'wt') as fout:
        with gzip.open(fileTriples, "rt") as fileIn:
            for line in fileIn:
                _line = line.replace("\n", "").split("\t")
                triple = _line[65:len(_line)]
                pred = triple[6].split(" :")[0]
                if len(setPredicates)>0:
                    try:
                        num=setPredicates.get(pred)
                    except KeyError:
                        pass
                    if num==None:
                        continue
                    if num>=5:
                        try:
                            del setPredicates[pred]
                            print('Len predicates: ', len(setPredicates))
                        except KeyError:
                            pass
                    else:
                        print("Num: ", num)
                        dictPredicates[pred]+=line
                        setPredicates[pred]+=1
                else:
                    break;
        for k, v in dictPredicates.items():
            if v!='':
                fout.write(v)

if __name__ == '__main__':
    args = sys.argv[1:]
    extractExampleTriples(args[0],args[1])