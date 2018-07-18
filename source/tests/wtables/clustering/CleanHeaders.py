import re
import nltk
nltk.download('wordnet')
from nltk.stem.snowball import SnowballStemmer

stemmer = SnowballStemmer("english")
def stemm_text(text):
    s=str(text).split(",")
    sw=[]
    for h in s:
        if h=="":
            continue
        hs=h.split()
        if len(hs)>1:
            sw.append(" ".join([stemmer.stem(str(x.strip())) for x in hs if x.strip()!=""]))
        else:
            sw.append(h)
    return ",".join([stemmer.stem(str(x)) for x in sw if x!=""])



inFileName="headersFile.out"
outFile=open("outFile.csv", "w", encoding="utf-8")
with open(inFileName, "r",encoding="utf-8") as inFile:
    cont=0
    for line in  inFile:
        cont+=1
        print("cont:", cont)
        lh=line.replace("\n","").split("\t")[4].split(",")
        cleanheader=""
        for wl in lh:
            text = re.sub("\W+", " ", wl)
            if text!="":
                cleanheader+=text+","
        text=stemm_text(cleanheader)
        text=text.split(",")
        numberHeaders=set()
        yearHeaders=set()
        nh=[]
        for w in text:
            if w==" ":
                continue
            if re.match("\d", w.strip()):
                if re.match(r'.*([1-3][0-9]{3})', w):
                    yearHeaders.add(w)
                else:
                    numberHeaders.add(w)
            else:
                nh.append(w)
        if len(numberHeaders)>=1:
            nh.append("numericheader")
        if len(yearHeaders)>1:
            nh.append("yearheader")
        nh.sort()
        if len(nh)>=1:
            outFile.write(line.replace("\n","")+"\t"+",".join(nh)+"\n")

outFile.close()