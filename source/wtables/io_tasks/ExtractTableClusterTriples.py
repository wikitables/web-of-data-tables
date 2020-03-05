import gzip
import sys

def extractTriples(fileTriples):
    cont=0
    with gzip.open('triplesClusters.csv.gz','wt') as tcout:
        with gzip.open('triplesTables.csv.gz','wt') as ttout:
            with gzip.open(fileTriples, "rt") as fin:
                for line in fin:
                    print("Line: ", cont)
                    cont+=1
                    _line=line.replace("\n", "").split("\t")
                    isFromTable=_line[37]
                    if isFromTable=='0':
                        ttout.write(line)
                    else:
                        if isFromTable == '2':
                            tcout.write(line)

if __name__=='__main__':
    args= sys.argv[1:]
    extractTriples(args[0])



