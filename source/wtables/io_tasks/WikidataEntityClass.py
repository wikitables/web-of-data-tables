import sys
import gzip

def extractEntityInstances(allEntityClass, linksEntities):
    classes=set()
    oldEntity=""
    line_count=0
    dictEntities={}
    with open(linksEntities, 'r') as fe:
        for line in fe:
            split = line.replace("\n", "").split("\t")
            entity = split[1]
            dictEntities[entity]=""
    line_count=0
    with gzip.open('wikidataEntityClasses.txt.gz', 'wt') as fout:
        with gzip.open(allEntityClass, 'rt') as fin:
            for line in fin:
                print("Line: ", line_count)
                line_count+=1
                split=line.replace("\n", "").split("\t")
                entity=split[0]
                if dictEntities.get(entity)!=None:
                    fout.write(line)

if __name__ == '__main__':
    args = sys.argv[1:]
    extractEntityInstances(args[0], args[1])