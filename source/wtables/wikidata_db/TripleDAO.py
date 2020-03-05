from wtables.wikidata_db.Connection import Connection
import sys
from datetime import datetime
import numpy as np
import gzip
import traceback
def insertTriplesFromFile(fileTriples, user, source, exist, cluster_relation):
    """
    Insert triples into db. table_triples.

    """
    connection=Connection()
    connection.connect()
    cur = connection.conn.cursor()
    sql = """INSERT INTO table_triples(cluster, table_id, index_col1, index_col2,
            index_row, name_col1, name_col2, 
            subj, subj_id, obj, obj_id, 
            pred, pred_id, exist, username, 
            date,cluster_relation, features, 
            source, triple_id) VALUES 
            (%s, %s, %s, %s, %s, %s,%s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s,%s);"""
    with open(fileTriples, "r") as fileT:
        try:
            for line in fileT:
                _line=np.array(line.replace("\n","").split("\t"))
                features = "\t".join(_line[0:66])
                triple=_line[66:len(_line)]
                cluster=triple[0]
                table_id = triple[1]
                row_col = triple[2].split(":")
                index_row=row_col[0]
                print (triple)
                index_col1 = row_col[1]
                index_col2 = row_col[2]
                name_col1 = triple[3]
                name_col2 = triple[4]
                subj = triple[5].split(" :")
                subj_uri=subj[0]
                subj_id = subj[1]
                subj = triple[5]
                pred = triple[6].split(" :")
                pred_uri="https://www.wikidata.org/wiki/Property:"+pred[0]
                pred_id=pred[0]
                pred_name = pred[1]
                pred = triple[6]
                obj = triple[7].split(" :")
                obj_uri=obj[0]
                obj_id = obj[1]
                obj = triple[7]

                #exist = #{"+str(",",join(features.split("\t")))+"}"

                dt = datetime.now()
                #cur.execute("select * from table_triples")
                #print(cur.fetchone())
                #table_id, index_col1, index_col2, index_row, name_col1, name_col2,
                #subj_id, obj_id, pred_id, triple_id
                cur.execute(sql,
                            (cluster, table_id, index_col1, index_col2, index_row,
                             name_col1, name_col2, subj, subj_id, obj,
                             obj_id, pred, pred_id, exist, user,
                             dt,cluster_relation, features, source, None))
                connection.conn.commit()
                # close communication with the database
                #cur.close()
        except Exception as ex:
            traceback.print_exc()

            print(ex)
        finally:
            if cur is not None:
                cur.close()
                connection.close()
    connection.close()

if __name__ == '__main__':
    args = sys.argv[1:]
    file="../features_cluster/test.csv"#args[0]
    user = "admin"#args[1]
    source= "CLUSTER_3"#args[2]
    exist = "0"#args[3]
    clusterEval = 1#args[4]
    if clusterEval=='1':
        clusterEval=True
    else:
        clusterEval=False
    insertTriplesFromFile(file, user, source, exist, clusterEval)