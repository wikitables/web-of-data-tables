# -*- coding: utf-8 -*-

import unittest

import pytest
from bs4 import BeautifulSoup

from wtables.preprocessing import ReadHTML as readHTML


class TestReadHTML(unittest.TestCase):

    def test_rowspan(self):
        html = """<table border="1" class="wikitable"><tr><td>1</td>
        <td colspan="2">2 and 3</td><td>4</td></tr>
        <tr><td rowspan="3">5,9 and 13</td>
        <td>6</td><td>7</td><td>8</td></tr>
        <tr><td>10</td><td>11</td><td>12</td></tr>
        <tr><td colspan="3">14,15 and 16</td></tr>
        </table>"""
        soup = BeautifulSoup(html, 'html.parser')
        listt= readHTML.tableTo2d(soup)
        table2d=listt[0]
        table2d = table2d.toHTML()
        self.assertFalse(table2d is None)

        result = """<table ><tr><td>1</td>
                <td>2 and 3</td><td>2 and 3</td><td>4</td></tr>
                <tr><td>5,9 and 13</td>
                <td>6</td><td>7</td><td>8</td></tr>
                <tr><td>5,9 and 13</td><td>10</td><td>11</td><td>12</td></tr>
                <tr><td>5,9 and 13</td><td>14,15 and 16</td><td>14,15 and 16</td><td>14,15 and 16</td></tr>
                </table>""".replace(" ", "").replace("\n", "")
        resultSoup=BeautifulSoup(table2d,"html.parser")
        resultSoup=readHTML.removeSpanAttrs(resultSoup)
        resultSoup = str(resultSoup).replace(" ", "").replace("\n", "")
        self.assertEqual(resultSoup,result)


    def test_without_span(self):
        html = """<table border="1" class="wikitable">
        <tr><td>1</td><td>2</td><td>3</td></tr>
        <tr><td>4</td><td>5</td><td>6</td></tr>
        </table>"""
        soup = BeautifulSoup(html, 'html.parser')
        listt = readHTML.tableTo2d(soup)
        table2d = listt[0]
        table2d=table2d.toHTML()
        self.assertFalse(table2d is None)
        table2dcontent = table2d.replace(" ", "").replace("\n", "")
        result = """<table>
        <tr><td>1</td><td>2 </td><td>3</td></tr>
        <tr><td>4</td><td>5</td><td>6</td></tr>
        </table>""".replace(" ", "").replace("\n", "")
        self.assertEqual(table2dcontent, result)


    def test_colspan(self):
        html = """<!DOCTYPE html><html>
            <head> <meta charset="utf-8"><meta name="description" content=""><meta name="keywords" content="">
                <title>Table Practice</title></head>
            <body><table class="wikitable" border="1" align="center" cellpadding="10px">
            <thead><tr><th rowspan="3">Day</th>
            <th colspan="3">Seminar</th></tr>
            <tr><th colspan="2">Schedule</th>
            <th rowspan="2">Topic</th></tr>
            <tr><th>Begin</th><th>End</th></tr></thead>
            <tbody><tr><td rowspan="2">Monday</td>
            <td rowspan="2">8:00 a.m</td><td rowspan="2">5:00 p.m</td>
            <td rowspan="">Introduction to XML</td></tr>
            <tr><td rowspan="">Validity: DTD and Relax NG</td>
            </tr><tr><td rowspan="4">Tuesday</td>
            <td>8:00 a.m</td><td>11:00 a.m</td>
            <td rowspan="2">XPath</td></tr><tr>
            <td rowspan="2">11:00 a.m</td>
            <td rowspan="2">2:00 p.m</td>
            </tr><tr><td rowspan="2">XSL transformation</td></tr>
            <tr><td>2:00 p.m</td><td>5:00 p.m</td></tr>
            <tr><td>Wednesday</td><td>8:00 a.m</td><td>12:00 p.m</td><td>XLS Formatting Objects</td>
            </tr></tbody></table>
            </body>
            </html>
            """
        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        lt = len(tables)
        self.assertEqual(lt, 1)
        listt = readHTML.tableTo2d(soup)
        table2d = listt[0]
        table2d = table2d.toHTML()
        self.assertFalse(table2d is None)
        table2dcontent = table2d.replace(" ", "").replace("\n", "")
        result = """<table>
        <tr><th>Day</th><th>Seminar</th><th>Seminar</th><th>Seminar</th></tr>
        <tr><th>Day</th><th>Schedule</th><th>Schedule</th><th>Topic</th></tr>
        <tr><th>Day</th><th>Begin</th><th>End</th><th>Topic</th></tr>
        <tr><td>Monday</td><td>8:00 a.m</td><td>5:00 p.m</td><td>Introduction to XML</td></tr>
        <tr><td>Monday</td><td>8:00 a.m</td><td>5:00 p.m</td><td>Validity: DTD and Relax NG</td></tr>
        <tr><td>Tuesday</td><td>8:00 a.m</td><td>11:00 a.m</td><td>XPath</td></tr>
        <tr><td>Tuesday</td><td>11:00 a.m</td><td>2:00 p.m</td><td>XPath</td></tr>
        <tr><td>Tuesday</td><td>11:00 a.m</td><td>2:00 p.m</td><td>XSL transformation</td></tr>
        <tr><td>Tuesday</td><td>2:00 p.m</td><td>5:00 p.m</td><td>XSL transformation</td></tr>
        <tr><td>Wednesday</td><td>8:00 a.m</td><td>12:00 p.m</td><td>XLS Formatting Objects</td></tr></table>""".replace(
            " ", "").replace("\n", "")
        print(table2dcontent)
        self.assertEqual(table2dcontent, result)

    @pytest.mark.skip
    def test_innerTable(self):
        html = """<table><tbody><tr valign="top"><td><table class="wikitable" style="font-size:95%"><tbody><tr bgcolor="#efefef">
        <td colspan="2"><b>Legend</b></td></tr><tr bgcolor="#f3e6d7"><td>Grand Slam</td><td align="center">0</td></tr><tr bgcolor="#ffffcc">
        <td>WTA Championships</td><td align="center">0</td></tr><tr bgcolor="#ffcccc"><td>Tier I</td><td align="center">0</td></tr>
        <tr bgcolor="#ccccff"><td>Tier II</td><td align="center">0</td></tr><tr bgcolor="#CCFFCC"><td>Tier III</td><td align="center">0
        </td></tr><tr bgcolor="#66CCFF"><td>Tier IV &amp; V</td><td align="center">0</td></tr></tbody></table></td><td><table class="wikitable" style="font-size:95%">
        <tbody><tr bgcolor="#efefef"><td colspan="2"><b>Titles by Surface</b></td></tr><tr><td>Hard</td>
        <td align="center">0</td></tr><tr><td>Clay</td><td align="center">0</td></tr><tr><td>Grass</td>
        <td align="center">0</td></tr><tr><td>Carpet</td><td align="center">0</td></tr></tbody></table></td></tr></tbody></table>"""
        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        listt = readHTML.tableTo2d(tables[0])
        table2d = listt[0]
        headers=readHTML.getMainColHeaders(table2d.htmlMatrix)
        self.assertFalse(table2d is None)
        table2d=table2d.toHTML()
        table2dcontent = table2d.replace(" ", "").replace("\n", "")

    def test_innerEqualTables(self):
        html ="""<table><tbody><tr><td width="10%" valign="top"><table class="wikitable">
        <tbody><tr><th width="150">Pool A</th><th width="15">W</th><th width="15">L</th></tr>
        <tr bgcolor="#ffffcc"><td><span class="flagicon"><a href="/wiki/Wisconsin" title="Wisconsin">
        <img alt="Wisconsin" src="//upload.wikimedia.org/wikipedia/commons/thumb/2/22/Flag_of_Wisconsin.svg/23px-Flag_of_Wisconsin.svg.png"
         width="23" height="15" class="thumbborder" 
         srcset="//upload.wikimedia.org/wikipedia/commons/thumb/2/22/Flag_of_Wisconsin.svg/35px-Flag_of_Wisconsin.svg.png 1.5x,
          //upload.wikimedia.org/wikipedia/commons/thumb/2/22/Flag_of_Wisconsin.svg/45px-Flag_of_Wisconsin.svg.png 2x" 
          data-file-width="675" data-file-height="450"></a></span> <a href="/wiki/Erika_Brown" 
          title="Erika Brown">Erika Brown</a></td><td>4</td><td>0</td></tr><tr bgcolor="#ffffcc"><td>
          <span class="flagicon"><a href="/wiki/Massachusetts" title="Massachusetts"><img alt="Massachusetts" 
          src="//upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Flag_of_Massachusetts.svg/23px-Flag_of_Massachusetts.svg.png" 
          width="23" height="14" class="thumbborder" 
          srcset="//upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Flag_of_Massachusetts.svg/35px-Flag_of_Massachusetts.svg.png 1.5x, 
          //upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Flag_of_Massachusetts.svg/46px-Flag_of_Massachusetts.svg.png 2x" data-file-width="1500" data-file-height="900">
          </a></span> <a href="/wiki/Korey_Dropkin" title="Korey Dropkin">Korey Dropkin</a></td><td>3</td><td>1</td></tr>
          <tr bgcolor="#ffffcc"><td><span class="flagicon"><a href="/wiki/Ontario" title="Ontario"><img alt="Ontario" 
          src="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/23px-Flag_of_Ontario.svg.png" width="23" 
          height="12" class="thumbborder" 
          srcset="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/35px-Flag_of_Ontario.svg.png 1.5x, 
          //upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/46px-Flag_of_Ontario.svg.png 2x" 
          data-file-width="2400" data-file-height="1200">
          </a></span> <a href="/w/index.php?title=Ben_Bevan&amp;action=edit&amp;redlink=1" class="new" 
          title="Ben Bevan (page does not exist)">Ben Bevan</a></td><td>2</td><td>2</td></tr><tr><td><span class="flagicon">
          <a href="/wiki/Minnesota" title="Minnesota"><img alt="Minnesota" src="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/23px-Flag_of_Minnesota.svg.png" width="23" height="15" class="thumbborder" 
          srcset="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/35px-Flag_of_Minnesota.svg.png 1.5x, 
          //upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/46px-Flag_of_Minnesota.svg.png 2x" 
          data-file-width="500" data-file-height="318"></a></span> <a href="/wiki/Cory_Christensen" 
          title="Cory Christensen">Cory Christensen</a></td><td>1</td><td>3</td></tr><tr><td><span class="flagicon"><a href="/wiki/Pennsylvania" title="Pennsylvania"><img alt="Pennsylvania" src="//upload.wikimedia.org/wikipedia/commons/thumb/f/f7/Flag_of_Pennsylvania.svg/23px-Flag_of_Pennsylvania.svg.png" width="23" height="15" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/f/f7/Flag_of_Pennsylvania.svg/35px-Flag_of_Pennsylvania.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/f/f7/Flag_of_Pennsylvania.svg/45px-Flag_of_Pennsylvania.svg.png 2x" data-file-width="675" data-file-height="450"></a></span> <a href="/w/index.php?title=Nicholas_Visnich&amp;action=edit&amp;redlink=1" class="new" title="Nicholas Visnich (page does not exist)">Nicholas Visnich</a></td><td>0</td><td>4</td></tr></tbody></table></td><td width="10%" valign="top"><table class="wikitable"><tbody><tr><th width="150">Pool B</th><th width="15">W</th><th width="15">L</th></tr><tr bgcolor="#ffffcc"><td><span class="flagicon"><a href="/wiki/Ontario" title="Ontario"><img alt="Ontario" src="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/23px-Flag_of_Ontario.svg.png" width="23" height="12" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/35px-Flag_of_Ontario.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/46px-Flag_of_Ontario.svg.png 2x" data-file-width="2400" data-file-height="1200"></a></span> <a href="/wiki/Scott_McDonald_(curler)" title="Scott McDonald (curler)">Scott McDonald</a></td><td>4</td><td>0</td></tr><tr bgcolor="#ffffcc"><td><span class="flagicon"><a href="/wiki/Minnesota" title="Minnesota"><img alt="Minnesota" src="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/23px-Flag_of_Minnesota.svg.png" width="23" height="15" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/35px-Flag_of_Minnesota.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/46px-Flag_of_Minnesota.svg.png 2x" data-file-width="500" data-file-height="318"></a></span> <a href="/wiki/Alexandra_Carlson" title="Alexandra Carlson">Alexandra Carlson</a></td><td>2</td><td>2</td></tr><tr bgcolor="#ccffcc"><td><span class="flagicon"><a href="/wiki/California" title="California"><img alt="California" src="//upload.wikimedia.org/wikipedia/commons/thumb/0/01/Flag_of_California.svg/23px-Flag_of_California.svg.png" width="23" height="15" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/0/01/Flag_of_California.svg/35px-Flag_of_California.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/0/01/Flag_of_California.svg/45px-Flag_of_California.svg.png 2x" data-file-width="900" data-file-height="600"></a></span> <a href="/w/index.php?title=Gabrielle_Coleman&amp;action=edit&amp;redlink=1" class="new" title="Gabrielle Coleman (page does not exist)">Gabrielle Coleman</a></td><td>2</td><td>2</td></tr><tr><td><span class="flagicon"><a href="/wiki/Ontario" title="Ontario"><img alt="Ontario" src="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/23px-Flag_of_Ontario.svg.png" width="23" height="12" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/35px-Flag_of_Ontario.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/46px-Flag_of_Ontario.svg.png 2x" data-file-width="2400" data-file-height="1200"></a></span> <a href="/w/index.php?title=Trevor_Brewer_(curler)&amp;action=edit&amp;redlink=1" class="new" title="Trevor Brewer (curler) (page does not exist)">Trevor Brewer</a></td><td>1</td><td>3</td></tr><tr><td><span class="flagicon"><a href="/wiki/Minnesota" title="Minnesota"><img alt="Minnesota" src="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/23px-Flag_of_Minnesota.svg.png" width="23" height="15" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/35px-Flag_of_Minnesota.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/46px-Flag_of_Minnesota.svg.png 2x" data-file-width="500" data-file-height="318"></a></span> <a href="/w/index.php?title=Ethan_Meyers&amp;action=edit&amp;redlink=1" class="new" title="Ethan Meyers (page does not exist)">Ethan Meyers</a></td><td>1</td><td>3</td></tr></tbody></table></td><td width="10%" valign="top"><table class="wikitable"><tbody><tr><th width="150">Pool C</th><th width="15">W</th><th width="15">L</th></tr><tr bgcolor="#ffffcc"><td><span class="flagicon"><a href="/wiki/Minnesota" title="Minnesota"><img alt="Minnesota" src="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/23px-Flag_of_Minnesota.svg.png" width="23" height="15" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/35px-Flag_of_Minnesota.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Flag_of_Minnesota.svg/46px-Flag_of_Minnesota.svg.png 2x" data-file-width="500" data-file-height="318"></a></span> <a href="/w/index.php?title=Mark_Haluptzok&amp;action=edit&amp;redlink=1" class="new" title="Mark Haluptzok (page does not exist)">Mark Haluptzok</a></td><td>4</td><td>0</td></tr><tr bgcolor="#ffffcc"><td><span class="flagicon"><a href="/wiki/Indiana" title="Indiana"><img alt="Indiana" src="//upload.wikimedia.org/wikipedia/commons/thumb/a/ac/Flag_of_Indiana.svg/23px-Flag_of_Indiana.svg.png" width="23" height="15" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/a/ac/Flag_of_Indiana.svg/35px-Flag_of_Indiana.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/a/ac/Flag_of_Indiana.svg/45px-Flag_of_Indiana.svg.png 2x" data-file-width="750" data-file-height="500"></a></span> <a href="/w/index.php?title=Greg_Eigner&amp;action=edit&amp;redlink=1" class="new" title="Greg Eigner (page does not exist)">Greg Eigner</a></td><td>3</td><td>1</td></tr><tr bgcolor="#ccffcc"><td><span class="flagicon"><a href="/wiki/New_York_(state)" title="New York (state)"><img alt="New York (state)" src="//upload.wikimedia.org/wikipedia/commons/thumb/1/1a/Flag_of_New_York.svg/23px-Flag_of_New_York.svg.png" width="23" height="12" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/1/1a/Flag_of_New_York.svg/35px-Flag_of_New_York.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/1/1a/Flag_of_New_York.svg/46px-Flag_of_New_York.svg.png 2x" data-file-width="900" data-file-height="450"></a></span> <a href="/w/index.php?title=Joyance_Meechai&amp;action=edit&amp;redlink=1" class="new" title="Joyance Meechai (page does not exist)">Joyance Meechai</a></td><td>2</td><td>2</td></tr><tr><td><span class="flagicon"><a href="/wiki/Massachusetts" title="Massachusetts"><img alt="Massachusetts" src="//upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Flag_of_Massachusetts.svg/23px-Flag_of_Massachusetts.svg.png" width="23" height="14" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Flag_of_Massachusetts.svg/35px-Flag_of_Massachusetts.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Flag_of_Massachusetts.svg/46px-Flag_of_Massachusetts.svg.png 2x" data-file-width="1500" data-file-height="900"></a></span> <a href="/w/index.php?title=Stephen_Dropkin&amp;action=edit&amp;redlink=1" class="new" title="Stephen Dropkin (page does not exist)">Stephen Dropkin</a></td><td>1</td><td>3</td></tr><tr><td><span class="flagicon"><a href="/wiki/Ontario" title="Ontario"><img alt="Ontario" src="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/23px-Flag_of_Ontario.svg.png" width="23" height="12" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/35px-Flag_of_Ontario.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/8/88/Flag_of_Ontario.svg/46px-Flag_of_Ontario.svg.png 2x" data-file-width="2400" data-file-height="1200"></a></span> <a href="/w/index.php?title=Gerry_Geurts&amp;action=edit&amp;redlink=1" class="new" title="Gerry Geurts (page does not exist)">Gerry Geurts</a></td><td>0</td><td>4</td></tr></tbody></table></td></tr></tbody></table>"""

        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        listt = readHTML.tableTo2d(tables[0])
        table2d = listt[0]
        headers = readHTML.getMainColHeaders(table2d.htmlMatrix)
        self.assertFalse(table2d is None)
        table2d = table2d.toHTML()
        table2dcontent = table2d.replace(" ", "").replace("\n", "")

    def test_interTitle(self):

        html = """<table class="x wikitable y" border="1" cellpadding="10px" align="center"><thead>
        <tr><th rowspan="3">A</th><th colspan="3">B</th></tr>
        <tr><th colspan="2">C</th><th rowspan="2">F</th></tr>
        <tr><th>D</th><th>E</th></tr></thead><tbody>
        <tr><td rowspan="2">1</td><td rowspan="2">a</td><td rowspan="2">b</td><td>x</td></tr>
        <tr><td>y</td></tr>
        <tr><td rowspan="4">2</td><td colspan="2">cd</td><td rowspan="2">z</td></tr>
        <tr><td rowspan="2">e</td><td rowspan="2">f</td></tr>
        <tr><td rowspan="2">w</td></tr>
        <tr><td>g</td><td>h</td></tr>
        <tr><th colspan="4">3</th></tr><tr>
        <td rowspan="4">4</td><td colspan="3">ijr</td></tr>
        <tr><td rowspan="2">5</td><td colspan="2">m</td></tr>
        <tr><td rowspan="2">n</td><td colspan="2">s</td></tr>
        <tr><td>6</td> <td>t</td></tr></tbody></table>"""
        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        lt = len(tables)
        self.assertEqual(lt, 1)
        listt = readHTML.tableTo2d(soup)
        table2d = listt[0]
        table2d=table2d.toHTML()
        self.assertFalse(table2d is None)
        table2dcontent = table2d.replace(" ", "").replace("\n", "")
        result = """<table>
        <tr><th>A</th><th>B</th><th>B</th><th>B</th></tr>
        <tr><th>A</th><th>C</th><th>C</th><th>F</th></tr>
        <tr><th>A</th><th>D</th><th>E</th><th>F</th></tr>
        <tr><td>1</td><td>a</td><td>b</td><td>x</td></tr>
        <tr><td>1</td><td>a</td><td>b</td><td>y</td></tr>
        <tr><td>2</td><td>cd</td><td>cd</td><td>z</td></tr>
        <tr><td>2</td><td>e</td><td>f</td><td>z</td></tr>
        <tr><td>2</td><td>e</td><td>f</td><td>w</td></tr>
        <tr><td>2</td><td>g</td><td>h</td><td>w</td></tr>
        <tr><th>3</th><th>3</th><th>3</th><th>3</th></tr>
        <tr><td >4</td><td >ijr</td><td >ijr</td><td >ijr</td></tr>
        <tr><td >4</td><td >5</td><td >m</td><td >m</td></tr>
        <tr><td >4</td><td >5</td><td >n</td><td >s</td></tr>
        <tr><td >4</td><td >6</td><td >n</td><td >t</td></tr>
        </table>""".replace(
            " ", "").replace("\n", "")
        print(table2dcontent)
        print(result)
        self.assertEqual(table2dcontent, result)

    def testCaption(self):
        html = """<table class="wikitable" style="text-align: center;float:right;">
        <caption>Irish stadiums in 1999 World Cup</caption><tbody><tr>
        <td><b>City</b></td><td><b>Stadium</b></td><td><b>Capacity</b>
        </td></tr><tr><td><span class="flagicon"><a href="/wiki/Republic_of_Ireland" title="Republic of Ireland"><img alt="Republic of Ireland" src="//upload.wikimedia.org/wikipedia/commons/thumb/4/45/Flag_of_Ireland.svg/23px-Flag_of_Ireland.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/4/45/Flag_of_Ireland.svg/35px-Flag_of_Ireland.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/4/45/Flag_of_Ireland.svg/46px-Flag_of_Ireland.svg.png 2x" data-file-width="1200" data-file-height="600" width="23" height="12"></a></span> <a href="/wiki/Dublin" title="Dublin">Dublin</a></td>
        <td><a href="/wiki/Lansdowne_Road" title="Lansdowne Road">Lansdowne Road</a></td>
        <td>49,250</td></tr><tr><td><span class="flagicon"><a href="/wiki/Republic_of_Ireland" title="Republic of Ireland"><img alt="Republic of Ireland" src="//upload.wikimedia.org/wikipedia/commons/thumb/4/45/Flag_of_Ireland.svg/23px-Flag_of_Ireland.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/4/45/Flag_of_Ireland.svg/35px-Flag_of_Ireland.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/4/45/Flag_of_Ireland.svg/46px-Flag_of_Ireland.svg.png 2x" data-file-width="1200" data-file-height="600" width="23" height="12"></a></span> <a href="/wiki/Limerick" title="Limerick">Limerick</a></td>
        <td><a href="/wiki/Thomond_Park" title="Thomond Park">Thomond Park</a></td>
        <td>13,500</td></tr><tr><td><span class="flagicon"><a href="/wiki/United_Kingdom" title="United Kingdom"><img alt="United Kingdom" src="//upload.wikimedia.org/wikipedia/en/thumb/a/ae/Flag_of_the_United_Kingdom.svg/23px-Flag_of_the_United_Kingdom.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/a/ae/Flag_of_the_United_Kingdom.svg/35px-Flag_of_the_United_Kingdom.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/a/ae/Flag_of_the_United_Kingdom.svg/46px-Flag_of_the_United_Kingdom.svg.png 2x" data-file-width="1200" data-file-height="600" width="23" height="12"></a></span>  <a href="/wiki/Belfast" title="Belfast">Belfast</a></td>
        <td><a href="/wiki/Ravenhill_Stadium" class="mw-redirect" title="Ravenhill Stadium">Ravenhill Stadium</a></td>
        <td>12,500</td></tr></tbody></table>"""
        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        listt = readHTML.tableTo2d(soup)
        table2d = listt[0]
        headers = readHTML.getMainColHeaders(table2d.htmlMatrix)
        self.assertFalse(table2d is None)
        table2dcontent = table2d.toHTML().replace(" ", "").replace("\n", "")

    def testHeaders(self):
        html="""<table class="wikitable sortable jquery-tablesorter">
            <thead></thead><tbody><tr><td>Season</td><td>2017</td>
            <td>2018</td><td><b>Total</b></td></tr><tr align="center"><td>Wins
            </td><td>1</td><td>0</td><td><b>1</b></td></tr></tbody><tfoot></tfoot></table>"""
        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        listt = readHTML.tableTo2d(soup)
        table2d = listt[0]
        headers = readHTML.getMainColHeaders(table2d.htmlMatrix)

    def testHeadersMix(self):
        html="""<table border="0">

<tbody><tr>
<td style="background-color:#FFFFFF;vertical-align:top;" width="48%">
<table cellspacing="0" cellpadding="2" border="0">


<tbody><tr style="background-color:#AAD0FF">
<th width="1%">No.
</th>
<th width="1%">
</th>
<th width="1%">Position
</th>
<th width="75%">Player
</th></tr>
<tr class="vcard agent">
<td style="text-align: right;">1
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Goalkeeper_(association_football)" title="Goalkeeper (association football)">GK</a>
</td>
<td><span class="fn"><a href="/wiki/Andreas_Isaksson" title="Andreas Isaksson">Andreas Isaksson</a></span> <i>(<a href="/wiki/Captain_(association_football)" title="Captain (association football)">vice-captain</a>)</i>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">2
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Johan_Andersson_(footballer,_born_1995)" title="Johan Andersson (footballer, born 1995)">Johan Andersson</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">3
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Marcus_Danielson" title="Marcus Danielson">Marcus Danielson</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">4
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Jacob_Une_Larsson" title="Jacob Une Larsson">Jacob Une Larsson</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">5
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Norway" title="Norway"><img alt="Norway" src="//upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Flag_of_Norway.svg/21px-Flag_of_Norway.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Flag_of_Norway.svg/32px-Flag_of_Norway.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Flag_of_Norway.svg/41px-Flag_of_Norway.svg.png 2x" data-file-width="1100" data-file-height="800" width="21" height="15"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Niklas_Gunnarsson" title="Niklas Gunnarsson">Niklas Gunnarsson</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">6
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Jesper_Karlstr%C3%B6m" title="Jesper Karlström">Jesper Karlström</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">7
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/D%C5%BEenis_Kozica" title="Dženis Kozica">Dženis Kozica</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">8
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Kevin_Walker_(Swedish_footballer)" title="Kevin Walker (Swedish footballer)">Kevin Walker</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">9
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Bosnia_and_Herzegovina" title="Bosnia and Herzegovina"><img alt="Bosnia and Herzegovina" src="//upload.wikimedia.org/wikipedia/commons/thumb/b/bf/Flag_of_Bosnia_and_Herzegovina.svg/23px-Flag_of_Bosnia_and_Herzegovina.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/b/bf/Flag_of_Bosnia_and_Herzegovina.svg/35px-Flag_of_Bosnia_and_Herzegovina.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/b/bf/Flag_of_Bosnia_and_Herzegovina.svg/46px-Flag_of_Bosnia_and_Herzegovina.svg.png 2x" data-file-width="800" data-file-height="400" width="23" height="12"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Haris_Radetinac" title="Haris Radetinac">Haris Radetinac</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">10
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Forward_(association_football)" title="Forward (association football)">FW</a>
</td>
<td><span class="fn"><a href="/wiki/Kerim_Mrabti" title="Kerim Mrabti">Kerim Mrabti</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">11
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Jonathan_Ring" title="Jonathan Ring">Jonathan Ring</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">12
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Forward_(association_football)" title="Forward (association football)">FW</a>
</td>
<td><span class="fn"><a href="/wiki/Omar_Eddahri" title="Omar Eddahri">Omar Eddahri</a></span>
</td></tr></tbody></table>
</td>
<td width="1%">
</td>
<td style="background-color:#FFFFFF;vertical-align:top;" width="48%">
<table cellspacing="0" cellpadding="2" border="0">


<tbody><tr style="background-color:#AAD0FF">
<th width="1%">No.
</th>
<th width="1%">
</th>
<th width="1%">Position
</th>
<th width="75%">Player
</th></tr>
<tr class="vcard agent">
<td style="text-align: right;">13
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Jonas_Olsson_(footballer,_born_1983)" title="Jonas Olsson (footballer, born 1983)">Jonas Olsson</a></span> <i>(<a href="/wiki/Captain_(association_football)" title="Captain (association football)">captain</a>)</i>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">14
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Besard_Sabovic" title="Besard Sabovic">Besard Sabovic</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">15
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Jonathan_Augustinsson" title="Jonathan Augustinsson">Jonathan Augustinsson</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">16
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn">Joseph Ceesay</span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">17
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Hampus_Finndell" title="Hampus Finndell">Hampus Finndell</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">18
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Zambia" title="Zambia"><img alt="Zambia" src="//upload.wikimedia.org/wikipedia/commons/thumb/0/06/Flag_of_Zambia.svg/23px-Flag_of_Zambia.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/0/06/Flag_of_Zambia.svg/35px-Flag_of_Zambia.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/0/06/Flag_of_Zambia.svg/45px-Flag_of_Zambia.svg.png 2x" data-file-width="2100" data-file-height="1400" width="23" height="15"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Edward_Chilufya" title="Edward Chilufya">Edward Chilufya</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">19
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Nicklas_B%C3%A4rkroth" title="Nicklas Bärkroth">Nicklas Bärkroth</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">20
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Senegal" title="Senegal"><img alt="Senegal" src="//upload.wikimedia.org/wikipedia/commons/thumb/f/fd/Flag_of_Senegal.svg/23px-Flag_of_Senegal.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/f/fd/Flag_of_Senegal.svg/35px-Flag_of_Senegal.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/f/fd/Flag_of_Senegal.svg/45px-Flag_of_Senegal.svg.png 2x" data-file-width="900" data-file-height="600" width="23" height="15"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Forward_(association_football)" title="Forward (association football)">FW</a>
</td>
<td><span class="fn"><a href="/wiki/Aliou_Badji" title="Aliou Badji">Aliou Badji</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">21
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Defender_(association_football)" title="Defender (association football)">DF</a>
</td>
<td><span class="fn"><a href="/wiki/Erik_Johansson_(footballer,_born_1988)" title="Erik Johansson (footballer, born 1988)">Erik Johansson</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">23
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Norway" title="Norway"><img alt="Norway" src="//upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Flag_of_Norway.svg/21px-Flag_of_Norway.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Flag_of_Norway.svg/32px-Flag_of_Norway.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Flag_of_Norway.svg/41px-Flag_of_Norway.svg.png 2x" data-file-width="1100" data-file-height="800" width="21" height="15"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Midfielder" title="Midfielder">MF</a>
</td>
<td><span class="fn"><a href="/wiki/Fredrik_Ulvestad" title="Fredrik Ulvestad">Fredrik Ulvestad</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">30
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Goalkeeper_(association_football)" title="Goalkeeper (association football)">GK</a>
</td>
<td><span class="fn"><a href="/wiki/Tommi_Vaiho" title="Tommi Vaiho">Tommi Vaiho</a></span>
</td></tr>
<tr class="vcard agent">
<td style="text-align: right;">35
</td>
<td style="text-align: right;"><span class="flagicon"><a href="/wiki/Sweden" title="Sweden"><img alt="Sweden" src="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/23px-Flag_of_Sweden.svg.png" class="thumbborder" srcset="//upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/35px-Flag_of_Sweden.svg.png 1.5x, //upload.wikimedia.org/wikipedia/en/thumb/4/4c/Flag_of_Sweden.svg/46px-Flag_of_Sweden.svg.png 2x" data-file-width="1600" data-file-height="1000" width="23" height="14"></a></span>
</td>
<td style="text-align: center;"><a href="/wiki/Goalkeeper_(association_football)" title="Goalkeeper (association football)">GK</a>
</td>
<td><span class="fn"><a href="/wiki/Oscar_Jonsson_(footballer)" title="Oscar Jonsson (footballer)">Oscar Jonsson</a></span>
</td></tr></tbody></table>
</td></tr></tbody></table>"""
        soup = BeautifulSoup(html, 'html.parser')
        tables = readHTML.readTables(soup)
        listt = readHTML.tableTo2d(tables[0])
        table2d = listt[0]
        headers = readHTML.getMainColHeaders(table2d.htmlMatrix)
        print(headers)

if __name__ == '__main__':
    unittest.main()
