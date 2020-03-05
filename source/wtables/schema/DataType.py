# -*- coding: utf-8 -*-

import re
import enum
from bs4 import BeautifulSoup
from dateutil.parser import parse

class DataType(enum.Enum):
    numeric = 1
    date = 2
    string = 3
    other=4


def getDataType(data):
    data = data.strip()
    if data == "":
        return None
    value = data.replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("─","-")

    if "-" in value or "/" in value or "–" in value:
        vdate=value.replace("–","-")
        try:
            if len(vdate.split("-"))>2 or len(vdate.split("/"))>2:
                try:
                    vdate = parse(value).strftime("%Y%m%d")
                    if vdate != None:
                        return DataType.date.value
                except:
                    pass
            else:
                vdate=vdate.replace(" ","")
                date=re.match(r'^[\d]{2}(\-)?(/)?[\d]{4}$', vdate)
                if date==None:
                    date = re.match(r'^[\d]{4}(-)?(/)?[\d]{2}$', vdate)
                    if date!=None:
                        return DataType.date.value
                    else:
                        date = re.match(r'^[\d]{4}(-)[\d]{4}$', vdate)
                        if date!=None:
                            return DataType.date.value
                else:
                    return DataType.date.value
        except:
            pass
    vdate=value.replace(" ","")
    if len(vdate) == 4:
        try:
            if int(vdate) <= 2050 and int(vdate) >= 1700:
                return DataType.date.value
        except:
            pass
    value = re.sub(r'[^(\w)?+(\d+\,\d)?+(\d+\.\d+)+(\w)?+(\s)?]*', "", value)
    value = value.strip()
    if value.endswith("."):
        value = value.replace(".", "")
    decimal = re.match(r'^((([\d]+\,+[\d]*)?)+([\d]+[\.]+[\d]+)*)$', value)
    integer = re.match(r'^((\d)+(\,[\d]{3})*?)$', value)
    if integer != None and integer != "":
        return DataType.numeric.value
    if decimal != None and decimal != "":
        return DataType.numeric.value
    try:
        intval=int(value.replace(" ","").replace(".","").replace(",",""))
        return DataType.numeric.value
    except:
        pass
    try:
        if len(vdate)>3:
            vdate = parse(vdate).strftime("%Y%m%d")
            if vdate != None:
                return DataType.date.value
    except:
        pass

    return DataType.string.value

if __name__ == '__main__':
    type=getDataType("dadad")
    assert type == DataType.string.value
    type = getDataType("afsdf $4.5 dadad")
    assert type == DataType.string.value
    type = getDataType("4.5 dadad")
    assert type == DataType.string.value
    type = getDataType("4.")
    assert type == DataType.numeric.value
    type = getDataType("% 4,000 $")
    assert type == DataType.numeric.value
    type = getDataType("% 45 $ 244,500")
    assert type == DataType.numeric.value
    type = getDataType(" 45  244,500 454.111")
    assert type == DataType.numeric.value
    type = getDataType(" 45,000.0 ")
    assert type == DataType.numeric.value
    type = getDataType(" 5,454,454.454")
    assert type == DataType.numeric.value
    type = getDataType(" %  / & * 553.55 ")
    assert type == DataType.numeric.value
    type = getDataType("10─jan")
    assert type == DataType.date.value
    type = getDataType("10─2015")
    assert type == DataType.date.value
    type = getDataType("1999─2015")
    assert type == DataType.date.value
    type = getDataType("15 january 2010")
    assert type == DataType.date.value
    type = getDataType("january 15 2010")
    assert type == DataType.date.value
    type = getDataType("1900–01")
    assert type == DataType.date.value
    type = getDataType("1900-01")
    assert type == DataType.date.value