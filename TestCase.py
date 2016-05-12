import requests
import json
import time
import MySQLdb
import traceback

class TestCase(object):
    
    def __init__(self,path,url):
        self.path = path
        self.url_base = url
        self.name = path.split('/')[-1]   
    
    # Get the attributes from the properly formatted description file    
    def getAttributes(self):
        self.desc_lines = open('%s/%s_desc.txt'%(self.path, self.name)).read().split('\n')
        self.attributes = {}
        for line in self.desc_lines:
            if line.startswith('#'):
                continue
            self.attributes[line.split(':')[0]] = line.split(':')[1]
        self.input_path = '%s/%s_input.txt' %(self.path, self.name)
        self.key_path = '%s/%s_key.txt' %(self.path, self.name)
        self.input_text = open(self.input_path).read()
    
    # Read in key file
    def getKey(self):
        self.key_text = open(self.key_path).read().split('\n')
        self.cols = self.key_text.pop(0).split(',')[1:]
          
        self.key = {}
        for line in self.key_text:
            temp = line.split(',')
            row = temp.pop(0)
            self.key[row] = {}
            for col in self.cols:
                self.key[row][col] = temp[self.cols.index(col)]
                
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
                for col in self.cols:
                    query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %(col, self.job_id, uid)
                    cursor.execute(query)
                    datapoint = cursor.fetchall()
                    if datapoint == ():
                        self.result = False
                        self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tRecieved: %s\n' %(uid, col, self.key[uid][col], datapoint)
                        continue
                    datapoint = str(datapoint[0][0])
                    correct = self.key[uid][col] == datapoint
                    if not(correct):
                        self.result = False
                        self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tRecieved: %s\n' %(uid, col, self.key[uid][col], datapoint)
                    self.data[uid][col] = datapoint
        except Exception:
            print traceback.format_exc()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
        print self.log_text
        