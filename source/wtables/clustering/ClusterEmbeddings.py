import nltk
import re
import gzip

def setUpText(text):
    textc=re.match(r'\d{4}$',text)
    if textc is not None:
        if int(text)>=1500 and int(text)<=2100:
            return 'yearheader'
    else:
        textc = re.match(r'(\d+)$', text)
        if textc is not None:
            return 'numberheader'
    return text

def removeTextAdds(text):
    #textc = text.split("@")[0]
    if "__" in text:
        textc = text.split("__")[1]
    else:
        textc=text
    #textc = " ".join(textc.split("**"))
    return textc

def cleanText(text):
    _text = re.sub(r'[^\w\s]', ' ', text)
    _text = _text.replace("\n"," ")
    _text='_'.join(e for e in _text.split(" ") if re.match(r'(\d+)$', e) is None)
    if _text!="_":
        return _text
    else:
        return ""

def cleanColHeaders(colHeaders):
    _colHeaders = [removeTextAdds(h) for h in colHeaders if "spancol" not in h]
    cleanHeaders=[]
    for h in _colHeaders:
        try:
            type=h.split("@")[1]
        except IndexError as  err:
            print("Error header: " ,h,_colHeaders,colHeaders)
            continue
        words = re.split(r'[_**]',h)
        hclean = ""
        for w in words:

            if w !='':
                _w=setUpText(w)
                _w=cleanText(_w)
                hclean += _w + "_"
        hclean=hclean[:-1]
        hclean=hclean.strip("_")
        if hclean!="" and hclean!="_":
            hclean += "@" + type
            if hclean not in cleanHeaders:
                cleanHeaders.append(hclean)
    cleanHeaders.sort()
    return cleanHeaders

def cleanHeader(fileTables):
    cont=0
    with open('tables_headers_clean.csv', "w") as fout:
        with open(fileTables,"r") as fin:
            for line in fin:
                if cont==0:
                    cont+=1
                    continue
                _line=line.replace("\n","").split("\t")
                headers=eval(_line[2])
                newHeaders=cleanColHeaders(headers)
                fout.write(_line[1]+"\t"+_line[2]+"\t"+str(newHeaders)+"\n")



def createVocab(fileTables):
    vocab={}
    cont=0
    with open(fileTables,"r") as fin:
        for line in fin:
            if cont==0:
                cont+=1
                continue
            _line=line.replace("\n","").split("\t")
            headers=eval(_line[2])
            for h in set(headers):
                if len(h)<=1:
                    continue
                hv=vocab.get(h)
                if hv is None:
                    vocab[h]=1
                else:
                    vocab[h]+=1

    with open('vocab_v1.txt', 'w') as fout:
        for k, v in vocab.items():
                fout.write(k+"\t"+str(v)+"\n")


def filterTables(fileTables, fileVocab):
    vocab={}
    with open(fileVocab,"r") as fin:
        for line in fin:
            _line=line.replace("\n","").split("\t")
            vocab[_line[0]]=int(_line[1])
    cont=0
    stopwords=['numberhead', 'yearhead']
    with gzip.open('tablesFiltered_4.csv.gz', "wt") as fout:
        with open(fileTables,"r") as fin:
            for line in fin:
                if cont==0:
                    cont+=1
                    continue
                _line=line.replace("\n","").split("\t")
                headers=eval(_line[2])
                headersv=[]
                for h in headers:
                    val=vocab.get(h)
                    stpwCheck=False
                    for stpw in stopwords:
                        if stpw in h:
                            stpwCheck=True
                            break
                    if not stpwCheck and val is not None and val>=100 and h not in headersv:
                        headersv.append(h)
                    else:
                        print(h)
                if len(headersv)>=1:
                    fout.write(_line[0]+"\t"+_line[1]+"\t"+str(list(headersv))+"\n")

#createVocab('tablesFiltered.csv')

#changeHeadersToVocab('tablesFiltered.csv', 'vocab_1.txt')
#print(cleanColHeaders(['dada**744@2','11-54/media/jhomara/Datos/MG-DCC/tesis/Desarrollo/web-of-data-tables/source/wtables/clustering/vocab_v1.txt@1','dsada@3','fdfdfa@5','1__dasda**dsadas#csd@3']))
#cleanHeader('tablesFiltered.csv')
#createVocab('tables_headers_clean.csv')
filterTables('tables_headers_clean.csv', 'vocab_v1.txt')