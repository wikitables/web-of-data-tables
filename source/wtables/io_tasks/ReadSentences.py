import gzip

def readFile(file):
    with gzip.open("sentences_tables.csv.gz", "wt") as fout:
        with gzip.open(file, "rt") as fin:
            for line in fin:
                _line=line.replace("\n","").split("\t")
                nameFile=_line[0]
                nameFile=nameFile.replace(".json","").replace("_",".")
                fout.write(nameFile+"\t"+line)

readFile("/home/jhomara/Desktop/sentences.csv.gz")

