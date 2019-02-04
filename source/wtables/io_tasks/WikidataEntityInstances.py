import gzip
import sys
def extractEntityInstances(fileTripleInstances):
    classes=set()
    oldEntity=""
    line_count=0
    with gzip.open('classesByInstances.out.gz', 'wt') as fout:
        with gzip.open(fileTripleInstances, "rt") as fileIn:
            for line in fileIn:
                print("Line: ", line_count)
                line_count+=1
                split=line.replace("\n", "").split("\t")
                entity=split[0]
                rel=split[1]
                if rel.strip()=="P31":
                    if entity==oldEntity:

                        classes.add(split[2])
                        print("classes", classes)
                    else:
                        if oldEntity!="":
                            fout.write(oldEntity+"\t"+str(list(classes))+"\n")
                        oldEntity=entity
                        classes =set()
                        classes.add(split[2])

if __name__ == '__main__':
    args = sys.argv[1:]
    extractEntityInstances(args[0])