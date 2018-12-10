from nltk.stem import PorterStemmer
import re

def removeStopWords(text):
    stopList = {'the', 'of', 'by', 'in', 'on', 'at', 'for', 'an'}
    textsplited=text.split("_")
    return " ".join([w for w in textsplited if w not in stopList])

def removeSpecialCharacters(text):
    #_text = text.replace("\'", "")
    _text = re.sub('\\s+', ' ', text)
    _text = _text.replace('&', ' and ').strip()
    _text = _text.replace('%', ' percentag ').strip()
    if len(_text)==1:
        if _text.isalnum(): return _text
        else: return ''
    else:
        textSplit=_text.split(" ")
        _text = ' '.join(e for e in textSplit if e.isalnum())
        #result = re.sub(r'[0-9]+', '', result)
        return _text
        #result = re.sub(r'[?|$|.|!\-\[\]/\(\)#,:]',r'', result)

def stemWord(text):
    ps = PorterStemmer()
    list1 = text.split(" ")
    result=""
    for w in list1:
        if w != "":
            result+= ps.stem(w)+" "

    return result.strip()

#print("dad",removeSpecialCharacters("%"))