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

def countInstancesByClass(fileClasses):
    count=0
    dictClasses={}
    with gzip.open(fileClasses, "rt") as fileIn:
            for line in fileIn:
                print("Line: ", count)
                count+=1
                instance=line.replace("\n","").split("\t")
                classes=eval(instance[1])
                for classi in classes:
                    if dictClasses.get(classi) is None:
                        dictClasses[classi]=1
                    else:
                        dictClasses[classi]+=1
    count=0
    with gzip.open('numInstancesByClass.out.gz', 'wt') as fout:
        with gzip.open(fileClasses, "rt") as fileIn:
            for line in fileIn:
                print("Line: ", count)
                count += 1
                instance = line.replace("\n", "").split("\t")
                classes = eval(instance[1])
                classesNumber={}
                for classi in classes:
                    classesNumber[classi]=dictClasses.get(classi)
                sortedClassesNumber = sorted(classesNumber.items(), key=lambda kv: kv[1])
                for classv in sortedClassesNumber:
                    classesNumber[classv[0]]=classv[1]
                fout.write(instance[0]+"\t"+str(classesNumber)+"\n")


if __name__ == '__main__':
    args = sys.argv[1:]
    countInstancesByClass(args[0])