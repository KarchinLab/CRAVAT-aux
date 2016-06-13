import MySQLdb
import traceback

db = MySQLdb.connect(host="192.168.99.100",
                         port=3306, 
                         user="root", 
                         passwd="1", 
                         db="cravat_results")
        
try:
    cursor = db.cursor()
    query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %('so_best', 'kmoad_20160520_094759', 'jk')
    cursor.execute(query)
    data = cursor.fetchone()
    print type(data)
   
except Exception:
    print traceback.format_exc()
    result = False
finally:
    try:
        cursor.close()
    except Exception:
        pass