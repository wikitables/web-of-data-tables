import sys
import gzip

def extractEntityInstances(allEntityClass, linksEntities, countClasses):

    line_count=0
    dictEntities={}
    with open(linksEntities, 'r') as fe:
        for line in fe:
            print('Enitities: ', line_count)
            line_count+=1
            split = line.replace("\n", "").split("\t")
            entity = split[1]
            dictEntities[entity]={}

    dictClasses={}
    line_count = 0
    with gzip.open(countClasses, 'rt') as fe:
        for line in fe:
            print('Count Classes: ', line_count)
            line_count += 1
            split = line.replace("\n", "").split(" ")
            classe = split[len(split)-1]
            count_classe=split[len(split)-2]
            print(classe,count_classe)
            dictClasses[classe]=count_classe


    with gzip.open(allEntityClass, 'rt') as fin:
            for line in fin:
                print("Entities classes: ", line_count)
                line_count+=1
                split=line.replace("\n", "").split("\t")
                entity=split[0]
                classes=eval(split[1])
                print("Len classes: ", len(classes), classes)
                if dictEntities.get(entity)!=None:
                    for classe in classes:
                        dictEntities[entity][classe]=dictClasses.get(classe)

    with gzip.open('wikidataEntityClassesCount.txt.gz', 'wt') as fout:
            for k, v in dictEntities.items():
                print("Writting: ", line_count)
                fout.write(k+"\t"+str(v)+"\n")


def maxClass():
    count=0
    with gzip.open('wikidataMaxEntityClass.txt.gz', 'wt') as fout:
        with gzip.open('wikidataEntityClassesCount.txt.gz', 'rt') as fin:
            for line in fin:
                split=line.replace("\n","").split("\t")
                entity=split[0]
                classes=eval(split[1])
                classes_sorted=sorted(classes.items(), key=lambda x: x[1], reverse=True)
                maxClass=''
                if len(classes_sorted)>0:
                    maxClass=classes_sorted[0][0]
                    print("Count", count, line, maxClass)
                count+=1
                fout.write(entity+"\t"+maxClass+"\n")




if __name__ == '__main__':
    args = sys.argv[1:]
    #extractEntityInstances(args[0], args[1], args[2])
    maxClass()