import gzip
import sys
import numpy as np

def searchTriples(fileToSearch, largeFile):
    triples=[]
    with open(fileToSearch, "r") as fin:
        for line in fin:
            _line=line.replace("\n","").split("\t")
            triples.append(_line)

    fout=open("triplesFound.csv","w" )
    cont=0
    with gzip.open(largeFile, "rt") as fin:
        for line in fin:
            if len(triples)==0:
                break;
            print("Line: ", cont)
            cont+=1
            _line=line.replace("\n","").split("\t")

            row=_line[68].split(":")[0]
            col1 = _line[68].split(":")[1]
            col2 = _line[68].split(":")[2]
            subj = _line[71].split(" :")[1]
            pred = _line[72].split(" :")[0]
            obj = _line[73].split(" :")[1]
            table=_line[67]
            #print(table, row, col1, col2, subj, pred, obj)
            for i in range(len(triples)):
                if i == 0:
                    continue
                t=np.array(triples[i])
                #print(t[0], t[1].split(":")[0], t[1].split(":")[1],t[1].split(":")[2],
                #      t[2].split(" :")[1],
                #      t[3].split(" :")[0],t[4].split(" :")[1])
                if t[0]==table and t[1].split(":")[0]==row \
                        and t[1].split(":")[1]==col1 and t[1].split(":")[2]==col2 \
                        and t[2].split(" :")[1]==subj and \
                        t[3].split(" :")[0]==pred and t[4].split(" :")[1]==obj:
                    print("found: ", 1)
                    fout.write(line)
                    triples.pop(i)
                    break
    fout.close()


def searchAnnotations(fileAnnotations, largeFile):
    triples=[]
    with open(fileAnnotations, "r") as fin:
        for line in fin:
            _line=line.replace("\n","").split("\t")
            triples.append(_line)

    fout=open("triplesAnnotated.csv","w" )
    cont=0
    with open(largeFile, "r") as fin:
        for line in fin:
            if len(triples)==0:
                break;
            print("Line: ", cont)
            cont+=1
            _line=line.replace("\n","").split("\t")

            row=_line[68].split(":")[0]
            col1 = _line[68].split(":")[1]
            col2 = _line[68].split(":")[2]
            subj = _line[71].split(" :")[1]
            pred = _line[72].split(" :")[0]
            obj = _line[73].split(" :")[1]
            table=_line[67]
            #print(table, row, col1, col2, subj, pred, obj)
            for i in range(len(triples)):
                if i == 0:
                    continue
                t=np.array(triples[i])
                #print(t[0], t[1].split(":")[0], t[1].split(":")[1],t[1].split(":")[2],
                #      t[2].split(" :")[1],
                #      t[3].split(" :")[0],t[4].split(" :")[1])
                if t[0]==table and t[1].split(":")[0]==row \
                        and t[1].split(":")[1]==col1 and t[1].split(":")[2]==col2 \
                        and t[2].split(" :")[1]==subj and \
                        t[3].split(" :")[0]==pred and t[4].split(" :")[1]==obj:
                    print("found: ", 1)
                    feat=_line[:66]
                    triple = _line[66:]
                    print(triple)
                    fout.write("\t".join([f.split(":")[1] for f in feat])+"\t"+
                               "\t".join(triple)+"\t"+t[5]+"\n")
                    triples.pop(i)
                    break
    fout.close()


if __name__ == '__main__':
    args = sys.argv[1:]
    searchAnnotations("/home/jhomara/Desktop/annotations.csv",
                  "/media/jhomara/Datos/MG-DCC/tesis/Desarrollo/web-of-data-tables/source/wtables/classification/triplesFound.csv")

