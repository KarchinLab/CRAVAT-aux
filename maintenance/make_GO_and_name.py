import os
import sys
import MySQLdb
import traceback
db_connections = {
             "local":{
                      "host": "localhost",
                      "user": "root",
                      "pwd" : "gizmo310"
                      }     
        }

def build_original_go():
    f = open('goa_human.gaf')
    gos = {}
    for line in f:
        if line[0] == '!':
            continue
        toks = line.split('\t')
        go = toks[4]
        gos[go] = True
    f.close()
    
    print len(gos.keys())
    
    go = ''
    f = open('go-basic.obo')
    for line in f:
        if len(line) < 6:
            continue
        
        if line[:6] == 'id: GO':
            go = line[:-1].split(' ')[1]
        if line[:5] == 'name:' and gos.has_key(go):
            gos[go] = line[:-1].split(':')[1][1:]
    f.close()
    
    wf = open('go_desc.txt', 'w')
    ordered_gos = gos.keys()
    ordered_gos.sort()
    for go in ordered_gos:
        wf.write(go + '\t' + gos[go] + '\n')
    wf.close()


def close(dbOrCursor):
    try:
        dbOrCursor.close()
    except Exception, q:
        pass
    
def build_input_txt(db, file_path):
    cursor = None
    wf = None
    try:
        wf = open(file_path, 'w')
        cursor = db.cursor()
        cursor.execute("select hugo, go from hugogo")
        hugos_gos = cursor.fetchall()
        for h_g in hugos_gos:
            hugo = h_g[0]
            gos_str = h_g[1]
            gos = gos_str.split(",")
            for go in gos:
                wf.write(str(hugo) + "\t" + str(go) + "\n")
    finally:
        close(cursor)
        wf.close()
        
    return
    
def update_go_database():
    db_name = "cravat_annot"
    db = None
    file_path = "/Users/derekgygax/Desktop/other/huogo_normalized.txt"
    try:
        db = MySQLdb.connect(host=db_connections['local']['host'],\
                         user=db_connections['local']['user'],\
                         passwd=db_connections['local']['pwd'],\
                         db=db_name)
        build_input_txt(db, file_path)
#         create_db_table()
#         load_table()
    except Exception:
        sys.stderr.write(str(traceback.format_exc()))
    finally:
        close(db)
        

if __name__ == "__main__":
# build_original_go()
    update_go_database()