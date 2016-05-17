import requests
import json
import time
import MySQLdb
import traceback
import math
import csv

class TestCase(object):
    
    def __init__(self,path,url):
        self.path = path
        self.url_base = url
        self.name = path.split('/')[-1]   
    
    def __data_parse(self,raw_tuple,col):
                
        if raw_tuple == ():
            return raw_tuple
        
        datapoint = raw_tuple[0][0]
        
        if col == 'cosmic_protein_change':
            # Just want to verify the protein change code
            return str(datapoint).split(' ')[0]
        elif col.startswith('exac'):
            # Round to 4 sig figs if it's a number. Remove trailing zero if it's zero.
            if type(datapoint) is float:
                if datapoint == 0:
                    data_rounded = 0
                else:
                    data_rounded = round(datapoint, int(4 - math.ceil(math.log10(abs(datapoint)))))
                return str(data_rounded)
            else:
                return str(datapoint)
        else:
            # Force the data into a string
            return str(datapoint)

    # Get the attributes from the properly formatted description file    
    def getAttributes(self):
        self.desc_lines = open('%s/%s_desc.txt'%(self.path, self.name)).read().split('\n')
        self.attributes = {}
        for line in self.desc_lines:
            if line.startswith('#'):
                continue
            self.attributes[line.split(':')[0]] = line.split(':')[1]
        self.input_path = '%s/%s_input.txt' %(self.path, self.name)
        self.key_path = '%s/%s_key.csv' %(self.path, self.name)
        self.input_text = open(self.input_path).read()
    
    # Read in key file
    def getKey(self):
        with open(self.key_path) as r:
            key_csv = csv.DictReader(r)
            self.key = {}
            for row in key_csv:
                self.key[row['uid']] = row
                del self.key[row['uid']]['uid'] 
                
    # Submit the job to cravat    
    def submitJob(self):        
        data = {
             'email':'kmoad@insilico.us.com',
             'analyses': self.attributes['analyses']
            }
        files = {
                'inputfile': open(self.input_path, 'r')
                }
        r = requests.post(self.url_base+'/rest/service/submit', files=files, data=data)
        self.job_id = json.loads(r.text)['jobid']
   
    # Check the status of the job.  Hold execution until job complete
    def checkStatus(self,sleep_time):
        self.job_status = ''
        while self.job_status == '':
            json_response = requests.get('%s/rest/service/status?jobid=%s' %(self.url_base, self.job_id))
            json_status = json.loads(json_response.text)['status']
            if json_status in ['Success', 'Salvaged', 'Error']:
                self.job_status = json_status
            
            time.sleep(sleep_time)
            
    # Verify that the entries in the key dictionary match the entries in the output SQL table
    def verify(self):
        self.result = True
        self.log_text = ''
        db = MySQLdb.connect(host="192.168.99.100",
                         port=3306, 
                         user="root", 
                         passwd="1", 
                         db="cravat_results")
        
        try:
            cursor = db.cursor()
            self.data = {}
            for uid in self.key:
                self.data[uid] = {}
                for col in self.key[uid]:
                    query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %(col, self.job_id, uid)
                    cursor.execute(query)
                    datapoint = self.__data_parse(cursor.fetchall(),col)
                    if datapoint == ():
                        self.result = False
                        self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tRecieved: %s\n' %(uid, col, self.key[uid][col], datapoint)
                        continue
                    correct = self.key[uid][col] == datapoint
                    if not(correct):
                        self.result = False
                        self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tRecieved: %s\n' %(uid, col, self.key[uid][col], datapoint)
                    self.data[uid][col] = datapoint
        except Exception:
            print traceback.format_exc()
            self.result = False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
        