import MySQLdb
import traceback

db = MySQLdb.connect(host="192.168.99.100",
                         port=3306, 
                         user="root", 
                         passwd="1", 
                         db="cravat_results")
        
try:
    cursor = db.cursor()
    query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %('exac_eas', 'kmoad_20160516_112412', 'exac1')
    cursor.execute(query)
    data = cursor.fetchall()
    print data
   
except Exception:
    print traceback.format_exc()
    result = False
finally:
    try:
        cursor.close()
    except Exception:
        pass