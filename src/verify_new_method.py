import MySQLdb
import traceback
import csv
import math

db = MySQLdb.connect(host="192.168.99.100",
                         port=3306, 
                         user="root", 
                         passwd="1", 
                         db="cravat_results")

key = {}
sig_figs = 4
log_text = ''
data = {}
job_id = 'kmoad_20160520_115730'
failures = 0

def attempt_round(entry,precision):
    try: 
        entry_num = float(entry)
        if entry_num == 0: 
            out = entry_num
        else: 
            out = round(entry_num, int(4 - math.ceil(math.log10(abs(entry_num)))))
    except: 
        out = str(entry)
#     print out, type(out)
    return out

# Read the key file into a 2D dictionary
key_path = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\pop_stats\\pop_stats_key.csv'
with open(key_path) as r:
    key_csv = csv.DictReader(r)
    for row in key_csv:
        uid = row['uid']
        key[uid] = row
        del key[uid]['uid']
        for entry in key[uid]:
            key[uid][entry] = attempt_round(key[uid][entry],sig_figs)
        
try:
    cursor = db.cursor()
    for uid in key:
        data[uid] = {}
        for col in key[uid]:
            query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %(col, job_id, uid)
            cursor.execute(query)
            # data_parse is needed to parse some columns
            data_tuple = cursor.fetchone()
            datapoint = attempt_round(data_tuple[0],sig_figs)
            correct = key[uid][col] == datapoint
            if datapoint == () or not(correct):
                failures +=1
                result = False
                log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tRecieved: %s\n' %(uid, col, key[uid][col], datapoint)
            data[uid][col] = datapoint
   
except Exception:
    print traceback.format_exc()
    result = False
finally:
    try:
        cursor.close()
    except Exception:
        pass

# for row in key:
#     print row
#     print key[row]
#     print data[row]

print 'Number of failures:', failures
print log_text
# print key['CYP19A1_NC']['exac_total'],data['CYP19A1_NC']['exac_total']
# print key['CYP19A1']['exac_nfe'],data['CYP19A1']['exac_nfe']