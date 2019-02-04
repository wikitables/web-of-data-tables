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


def random_sampler(ftables, ftriples, k, fout):
    sample = []
    fo=open(fout, "w")
    tables=[]
    with open(ftables, 'r') as f:
        for line in f:
            _line=line.replace("\n","").split("\t")
            tables.append(_line[0])

    with open(ftriples, 'r') as f:
        #linecount = sum(1 for line in f)
        #print("Line count: ", 30449860, k)
        f.seek(0)
        #n=30449860
        #lists=list(range(30449860))
        #random_linenos = sorted([random.randrange(0,n,1) for i in range(k)], reverse = True)
        #print("lines-->",random_linenos)
        #lineno = random_linenos.pop()
        for n, line in enumerate(f):
            _line=line.replace("\n","").split("\t")
            table=_line[1]
            if table in tables:
                tables.remove(table)
                fo.write(line.rstrip()+"\n")
                if len(tables) == 0:
                    break
            #if n == lineno:
                #print("Line: ", n)
                #print("pick:", lineno)
            #    fo.write(line.rstrip()+"\n")
            #    if len(random_linenos) > 0:
            #        lineno = random_linenos.pop()
            #    else:
            #        break
    fo.close()

if __name__ == '__main__':
    args = sys.argv[1:]
    #random_sampler(args[0], args[1], int(args[2]),args[2])
    random_sampler('tables.out','testtriples.out', 3, 'out.out')