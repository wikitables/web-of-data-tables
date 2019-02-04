from wtables.wikidata_db.Connection import Connection
import sys
from datetime import datetime
import gzip
def insertTriplesFromFile(fileTriples, user, eval, cluster_relation):
    connection=Connection()
    connection.connect()
    cur = connection.conn.cursor()
    sql = """INSERT INTO table_triples(table_id, index_col1, index_col2, index_row, name_col1, name_col2, 
            subj, subj_id, obj, obj_id, pred, pred_id, exist, evaluation,username, date,cluster_relation,triple_id) VALUES (%s,%s,%s,%s,
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s,%s,%s);"""
    with gzip.open(fileTriples, "rt") as fileT:
        try:
            for line in fileT:
                _line=line.replace("\n","").split("\t")
                cluster=_line[0]
                table_id = _line[1]
                row_col = _line[2].split(":")
                index_row=row_col[1]
                index_col1 = row_col[2]
                index_col2 = row_col[3]
                name_col1 = _line[3]
                name_col2 = _line[4]
                subj = _line[5].split(" :")
                subj_uri=subj[0]
                subj_id = subj[1]
                subj = _line[5]
                pred = _line[6].split(" :")
                pred_uri="https://www.wikidata.org/wiki/Property:"+pred[0]
                pred_id=pred[0]
                pred_name = pred[1]
                pred = _line[6]
                obj = _line[7].split(" :")
                obj_uri=obj[0]
                obj_id = obj[1]
                obj = _line[7]
                exist = _line[8]
                dt = datetime.now()
                #cur.execute("select * from table_triples")
                #print(cur.fetchone())
                #table_id, index_col1, index_col2, index_row, name_col1, name_col2,
                #subj_id, obj_id, pred_id, triple_id
                cur.execute(sql,
                            (table_id, index_col1, index_col2, index_row, name_col1, name_col2,
            subj, subj_id, obj, obj_id, pred, pred_id, exist,eval,user,dt,cluster_relation,None))
                connection.conn.commit()
                # close communication with the database
                #cur.close()
        except Exception as ex:
            print(ex)
        finally:
            if cur is not None:
                cur.close()
                connection.close()
    connection.close()

if __name__ == '__main__':
    args = sys.argv[1:]
    insertTriplesFromFile(args[0], args[1],args[2], args[3])