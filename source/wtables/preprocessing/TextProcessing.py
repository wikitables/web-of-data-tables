# -*- coding: utf-8 -*-

from nltk.stem.snowball import SnowballStemmer
from nltk.stem.porter import PorterStemmer
import re
#import textdistance
from similarity.jarowinkler import JaroWinkler
#from similarity.sorensen_dice import SorensenDice

class TextProcessing(object):

    def __init__(self):

        self.stemmer=SnowballStemmer("english")
        self.jaroWinkler = JaroWinkler()
        self.diceScore = None#SorensenDice()

    def removeStopWords(slef,text):
        stopList = {'the', 'of', 'by', 'in', 'on', 'at', 'for', 'an'}
        textsplited=text.replace("_"," ").split(" ")
        return " ".join([w for w in textsplited if w not in stopList])

    def removeSpecialCharacters2(self, text):
        #_text = text.replace("\'", "")
        pos = text.split("__")
        if len(pos) > 1:
            _text =pos[1]
            pos=pos[0]+"__"
        else:
            pos=''
            _text = text
        _text = _text.replace("_"," ")
        _text = _text.replace('&', ' and ').strip()
        _text = _text.replace('%', ' percentage ').strip()
        _text = _text.replace('#', ' no. ').strip()
        _text = _text.replace('$', ' currency ').strip()
        characters=['/','\\','>','<',"'s","(s)",'\"',"[","]","(",")","{","}","."]
        for c in characters:
            if c in _text:
                _text=_text.replace(c,' ').strip()
        _text = re.sub('\\s+', ' ', _text).strip()
        return pos+_text.replace(" ","_")
            #result = re.sub(r'[?|$|.|!\-\[\]/\(\)#,:]',r'', result)

    def stemWord(self, text):
        pos = text.split("__")
        if len(pos)>1:
            _text = pos[1]
            pos=pos[0]+"__"
        else:
            pos=''
            _text=text
        _text = " ".join(_text.split("_"))
        result= [self.stemmer.stem(t) for t in _text.split(" ")]
        result=" ".join(result)

        return pos+result.strip().replace(" ","_")

    def cleanForSimilarity(self, text):
        if "protag_article" in text:
            return ""
        if "__" in text:
            cleant=text.split("__")[1]
        else:
            cleant=text

        if " :" in cleant:
            cleant=cleant.split(" :")[1]
        if "@en" in cleant:
            cleant = cleant.split("@")[0]
            cleant=[self.stemmer.stem(t) for t in cleant.split(" ")]
            cleant="".join(cleant)

        cleant= cleant.replace("*","").replace("_","").replace("spancol","").split("@")[0]


        return cleant



    def textSimilarity(self,text1, text2):
        #print("text inicial: ", text1, text2)
        t1=self.cleanForSimilarity(text1)
        t2 = self.cleanForSimilarity(text2)
        if len(t1)==0 and len(t2)==0:
            return 0
        score1 = self.jaroWinkler.similarity(t1,t2)
        score2 = self.diceScore.similarity(t1,t2)
        mins= min([score1, score2])
        #print(t1,t2,mins)
        return mins


    def cleanCellHeader(self, cellText):
        _cellText = re.sub('\\s+', ' ', cellText).strip()
        _cellText=self.removeSpecialCharacters2(_cellText)
        _cellText=self.stemWord(_cellText)
        return _cellText

    def orderHeaders(self, headers):
        _headers=headers[:]
        hd = {hi: [] for hi in _headers}
        for i, hi in enumerate(_headers):
            hd[hi].append(i)
        headersD = {}
        for k, v in hd.items():
            if len(v) > 1:
                _v = v
                _v.sort()
                i = 1
                for posh in _v:
                    headersD[posh] = str(i) + "__" + k
                    i += 1
            else:
                headersD[v[0]] = k
        _headers = []
        for i in range(len(headers)):
            _headers.append(headersD.get(i))
        return _headers

    def cleanTableHeader(self, headers):
        dataTypes = [h.split("@")[len(h.split("@")) - 1] for h in headers]
        _headers = [h.split("@")[0] for h in headers]
        _headers = [self.removeSpecialCharacters2(h) for h in _headers]
        _headers = [self.stemWord(h) for h in _headers]
        _headers = ['spancol' if hi == "" else hi for hi in _headers]
        hd = {hi: [] for hi in _headers}
        for i, hi in enumerate(_headers):
            hd[hi].append(i)
        headersD = {}
        for k, v in hd.items():
            if len(v) > 1:
                _v = v
                _v.sort()
                i = 1
                for posh in _v:
                    headersD[posh] = str(i) + "__" + k
                    i += 1
            else:
                headersD[v[0]] = k
        _headers = []
        for k, v in headersD.items():
            _headers.append(v + "@" + dataTypes[k])
        return _headers


if __name__ == '__main__':
    tp=TextProcessing()
    print(tp.cleanForSimilarity("2__dadad@1"))
    print(tp.cleanForSimilarity("dadad**dad_adad@2"))
    print(tp.cleanForSimilarity("P3: dada d24234"))
    print(tp.textSimilarity("candid@3","successful candidate@en"))
    print(tp.stemmer.stem('easily'))
    #print(tp.textSimilarity('protag_article', 'has part'))
    """
    #print(tp.stemWord("runners_up"))
    #print(tp.cleanTableHeader(eval("['distance_(total;_km)@1', 'distance_(s2s;_km)@1', 'station_name_(transcribed)@3', 
                                        'station_name_(chosŏn'gŭl_(hanja))@3', 'former_name_(transcribed)@4', former_name_(chosŏn'gŭl_(hanja))@4, 'connections@3']")))
    fout=open("/home/jhomara/Desktop/web7/tableInformationNormalizedClean.out", "w")
    with open("/home/jhomara/Desktop/web7/tableInformationNormalized.out", "r") as filein:
        for line in filein:
            #print(line)
            _line=line.replace("\n","").split("\t")
            headers=eval(_line[1].replace('\"\"', ''))
            headers=tp.cleanTableHeader(headers)
            headers=[h for h in headers if 'spancol' not in h]
            if len(headers)>=2:
                fout.write(_line[0]+"\t"+str(headers)+"\t"+_line[3]+"\t"+_line[4]+"\n")
    fout.close()"""


