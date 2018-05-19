from bs4 import BeautifulSoup
from source.wtables.preprocessing import ReadHTML as readHTML
from source.wtables.preprocessing import ReadZipFile as readZip

import unittest
import pytest

class TestReadHTML(unittest.TestCase):

    @pytest.mark.skip
    def test_rowspan(self):
        html="""<table border="1" class="wikitable"><tr><td>1</td>
        <td colspan="2">2 and 3</td><td>4</td></tr>
        <tr><td rowspan="3">5,9 and 13</td>
        <td>6</td><td>7</td><td>8</td></tr> 
        <tr><td>10</td><td>11</td><td>12</td></tr>
        <tr><td colspan="3">14,15 and 16</td></tr>
        </table>"""
        soup = BeautifulSoup(html, 'html.parser')
        table2d, object = readHTML.tableTo2d(soup)
        self.assertFalse(table2d is None)
        table2dcontent = table2d.replace(" ","").replace("\n","")
        result="""<table ><tr><td>1</td>
                <td>2 and 3</td><td>2 and 3</td><td>4</td></tr>
                <tr><td>5,9 and 13</td>
                <td>6</td><td>7</td><td>8</td></tr> 
                <tr><td>5,9 and 13</td><td>10</td><td>11</td><td>12</td></tr>
                <tr><td>5,9 and 13</td><td>14,15 and 16</td><td>14,15 and 16</td><td>14,15 and 16</td></tr>
                </table>""".replace(" ", "").replace("\n", "")
        self.assertEqual(table2dcontent,result)


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
            <td>Introduction to XML</td></tr>
            <tr><td>Validity: DTD and Relax NG</td>
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
        self.assertEqual(lt,1)
        table2d, tableObject = readHTML.tableTo2d(tables[0])
        self.assertFalse(table2d is None)
        table2dcontent = table2d.replace(" ","").replace("\n","")
        result="""<table>
        <tr><th>Day</th><th>Seminar</th><th>Seminar</th><th>Seminar</th></tr>
        <tr><th>Day</th><th>Schedule</th><th>Schedule</th><th>Topic</th></tr>
        <tr><th>Day</th><th>Begin</th><th>End</th><th>Topic</th></tr>
        <tr><td>Monday</td><td>8:00 a.m</td><td>5:00 p.m</td><td>Introduction to XML</td></tr>
        <tr><td>Monday</td><td>8:00 a.m</td><td>5:00 p.m</td><td>Validity: DTD and Relax NG</td></tr>
        <tr><td>Tuesday</td><td>8:00 a.m</td><td>11:00 a.m</td><td>XPath</td></tr>
        <tr><td>Tuesday</td><td>11:00 a.m</td><td>2:00 p.m</td><td>XPath</td></tr>
        <tr><td>Tuesday</td><td>11:00 a.m</td><td>2:00 p.m</td><td>XSL transformation</td></tr>
        <tr><td>Tuesday</td><td>2:00 p.m</td><td>5:00 p.m</td><td>XSL transformation</td></tr>
        <tr><td>Wednesday</td><td>8:00 a.m</td><td>12:00 p.m</td><td>XLS Formatting Objects</td></tr></table>""".replace(" ","").replace("\n","")
        self.assertEqual(table2dcontent,result)

    def test_interTitle(self):
        html="""<table class="x wikitable y" border="1" cellpadding="10px" align="center"><thead>
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
        table2d, object = readHTML.tableTo2d(tables[0])
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

if __name__ == '__main__':
    unittest.main()
