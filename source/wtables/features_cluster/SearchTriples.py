import gzip
triples=[]
with open('triplesConcat.csv', "r") as fin:
    for line in fin:
        _line=line.replace("\n","").split("\t")
        triples.append(_line)
fout=open("featuresFound.csv","w" )
cont=0
with gzip.open('unsortedTriples0.csv.gz', "rt") as fin:
    for line in fin:
        print("Line: ", cont)
        cont+=1
        _line=line.replace("\n","").split("\t")
        features = _line[0:53]
        triple = _line[53:len(_line)]
        #print(triple)
        table=triple[1]
        row=triple[2].split(":")[0]
        col1 = triple[2].split(":")[1]
        col2 = triple[2].split(":")[2]
        subj=triple[5].split(" :")[1]
        obj = triple[7].split(" :")[1]
        pred = triple[6].split(" :")[0]
        if len(triples)==0:
            break
        for i, t in enumerate(triples):
            #print(table, subj, obj, pred, row)
            #print(t)
            if t[0]==table and t[1]==subj and t[2]==obj and t[3]==pred and t[4]==col1 and t[5]==col2 and t[6]==row:
                print("found: ", 1)
                fout.write(line)
                triples.pop(i)
                break
            else:
                if "-1" in pred:
                    p=pred.replace("-1","")
                else:
                    p=pred+"-1"
                if t[0] == table and t[1] == obj and t[2] == subj and t[3]==p and t[4]==col2 and t[5]==col1 and t[6]==row:
                    print("found: ", 1)
                    fout.write(line)
                    triples.pop(i)
                    break
fout.close()
