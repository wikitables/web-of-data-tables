import random
import gzip
import sys


def sampleTables(fileTables):
    df = pd.read_csv(fileTables, sep="\t", dtype={'table': str})
    # dictTest={k:[] for k in range(500)}
    #gcount = pd.DataFrame({'count':df.groupby('cluster').size()}).reset_index()
    indexTables = df.groupby('cluster')['table'].apply(lambda x: "{'%s'}" % "', '".join(x))
    indexTables = indexTables.to_dict()
    #gcount = gcount.to_dict()
    #print(gcount)
    tableSet = set()
    for k in range(500):
        try:
            index_k = eval(indexTables[k])
        except KeyError:
            continue
        pos = [int(random.uniform(0, len(index_k) - 1)) for n in range(5)]
        table1 = list(index_k)
        tableSet.add(table1[pos[0]])
        tableSet.add(table1[pos[1]])
    return tableSet


def random_sampler(ftriples, k, fout):
    sample = []
    fo=gzip.open(fout, "wt")
    tables=[]

    with gzip.open(ftriples, 'rt') as f:
        f.seek(0)
        n=65645207
        random_linenos = sorted([random.randrange(0,n,1) for i in range(k)], reverse = True)
        print("lines-->",random_linenos)
        lineno = random_linenos.pop()
        for n, line in enumerate(f):
            print("N: ", n)
            _line=line.replace("\n","").split("\t")
            if n == lineno:
                fo.write(line.rstrip()+"\n")
                if len(random_linenos) > 0:
                    lineno = random_linenos.pop()
                else:
                    break
    fo.close()

if __name__ == '__main__':
    args = sys.argv[1:]
    #random_sampler(args[0], args[1], int(args[2]),args[2])
    random_sampler(args[0], int(args[1]), args[2])
    #n = 65645207
    #random_linenos = sorted([random.randrange(0, n, 1) for i in range(100)], reverse=True)
    #print(random_linenos)