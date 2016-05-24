import MySQLdb
import traceback
import csv
import math
import os
import xml.etree.ElementTree as ET


db = MySQLdb.connect(host="192.168.99.100",
                         port=3306, 
                         user="root", 
                         passwd="1", 
                         db="cravat_results")

key = {}
sig_figs = 4
log_text = ''
data = {}
job_id = 'kmoad_20160523_104930'
failures = 0
path = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\rounding\\'

def recurse_xml(d):
    out = {}
    if len(d):
        for k in d:
            if len(k.attrib):
                out[k.attrib['name']] = recurse_xml(k)
            else:
                out[k.tag] = recurse_xml(k)
    else:
        out = d.text.strip()
    return out

def compare(datapoint, keypoint, method, modifier):    
    if method == 'string_exact':
        datapoint = str(datapoint)
        keypoint = str(keypoint)
        out = datapoint == keypoint
    
    elif method == 'string_truncate':
        modifier = int(modifier)
        datapoint = str(datapoint)
        keypoint = str(keypoint)
        out = datapoint[:modifier] == keypoint[:modifier]
    
    elif method == 'string_parse':
        modifier = str(modifier)
        datapoint = str(datapoint)
        keypoint = str(keypoint)
        out = keypoint == datapoint.split(modifier)[0] 
    
    elif method == 'float_sigfig':
        datapoint = float(datapoint)
        keypoint = float(keypoint)
        modifier = int(modifier)
        if datapoint != 0 and keypoint != 0:
            data_rounded = round(datapoint, int(modifier - math.ceil(math.log10(abs(datapoint)))))
            key_rounded = out = round(keypoint, int(modifier - math.ceil(math.log10(abs(keypoint)))))
            out = data_rounded == key_rounded
        else:
            out = round(datapoint,modifier) == round(keypoint,modifier)
    
    elif method == 'float_round':
        datapoint = float(datapoint)
        keypoint = float(keypoint)
        modifier = int(modifier)
        data_rounded = round(datapoint,modifier)
        key_rounded = out = round(keypoint,modifier)
        out = data_rounded == key_rounded
    
    elif method == 'float_truncate':
        datapoint = float(datapoint)
        keypoint = float(keypoint)
        modifier = int(modifier)
        temp = str(datapoint).split('.')
        temp[1] = temp[1][:modifier]
        data_rounded = float('.'.join(temp))
        temp = str(keypoint).split('.')
        temp[1] = temp[1][:modifier]
        key_rounded = float('.'.join(temp))
        out = data_rounded == key_rounded
    
    elif method == 'float_numeric_range':
        datapoint = float(datapoint)
        keypoint = float(keypoint)
        modifier = float(modifier)
        diff = abs(datapoint - keypoint)
        out = diff <= modifier
    
    elif method == 'float_percentage_range':
        datapoint = float(datapoint)
        keypoint = float(keypoint)
        if keypoint != 0:
            perc_diff = abs(datapoint - keypoint)/keypoint * 100
            out = perc_diff <= modifier
        else:
            out = False
    
    else:
        raise BaseException('Improper comparison method: %r. Check the syntax in the desc file.' %method)
    
    return out


# Read desc file into a nested dictionary
desc_path = os.path.join(path,'rounding_desc.xml')
cols = {}
with open(desc_path) as desc_file:
    desc_xml = ET.parse(desc_file).getroot()
    desc = recurse_xml(desc_xml)

# Read the key file into a 2D dictionary
key_path = os.path.join(path,'rounding_key.csv')
with open(key_path) as r:
    key_csv = csv.DictReader(r)
    for row in key_csv:
        uid = row['uid']
        key[uid] = row
        del key[uid]['uid']

## The following uses DATA READ FROM CSV FILE
# Read the data file into a 2D dictionary
data_path = os.path.join(path,'rounding_data.csv')
with open(data_path) as r:
    data_csv = csv.DictReader(r)
    for row in data_csv:
        uid = row['uid']
        data[uid] = row
        del data[uid]['uid']
for uid in key:
    for col in key[uid]:
        print '-'*10
        print '%s, %s' %(uid, col)
        
        if col in desc['verify_rules'].keys():
            method = desc['verify_rules'][col]['method']
            modifier = desc['verify_rules'][col]['modifier']
        else:
            method = 'string_exact'
            modifier = None
        
        datapoint = data[uid][col]
        keypoint = key[uid][col]
        
        print 'D: %r' %datapoint
        print 'K: %r' %keypoint
        print 'Method: %s' %method
        print 'Modifier: %r' %modifier
        
        correct = compare(datapoint, keypoint, method, modifier)
        print correct
        
        if not(correct):
            failures +=1
print '%s\nFailures: %d' %('-'*10,failures)
# ## The following uses DATA READ FROM THE DATABASE        
# try:
#     cursor = db.cursor()
#     for uid in key:
#         data[uid] = {}
#         for col in key[uid]:
#             query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %(col, job_id, uid)
#             cursor.execute(query)
#             datapoint = cursor.fetchone()[0]
#             print 'Datapoint: %r' %datapoint
#             try:
#                 keypoint = float(key[uid][col])
#             except:
#                 keypoint = key[uid][col]
#             print 'Keypoint: %r' %keypoint
#             if col in desc['verify_rules'].keys():
#                 method = desc['verify_rules'][col]['method']
#                 modifier = int(desc['verify_rules'][col]['modifier'])
#             else:
#                 method = 'string'
#                 modifier = None
#             print 'Method: %s' %method
#             print 'modifier: %r' %modifier
#             correct = compare(datapoint, keypoint, method, modifier)
#             print correct
#             data[uid][col] = datapoint
#             if datapoint == () or not(correct):
#                 failures +=1
#                 result = False
#                 log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tRecieved: %s\n' %(uid, col, key[uid][col], datapoint)
#     
# except Exception:
#     print traceback.format_exc()
#     result = False
# finally:
#     try:
#         cursor.close()
#     except Exception:
#         pass
#  
# print 'Number of failures:', failures
# print log_text