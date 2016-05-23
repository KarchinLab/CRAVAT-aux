import requests
import json
import time
import MySQLdb
import traceback
import math
import csv
import os

class TestCase(object):
    
    def __init__(self,path,url):
        self.path = path
        self.url_base = url
        self.name = os.path.basename(self.path)
        self.input_path = os.path.join(self.path,'%s_input.txt' %self.name)
        self.key_path = os.path.join(self.path,'%s_key.csv' %self.name)
        self.desc_path = os.path.join(self.path, '%s_desc.txt' %self.name)
        self.attributes = {}
        self.key = {}
        self.job_id = ''
        self.job_status = ''
        self.data = {}
        self.result = False
        self.log_text = ''

        # Open up the desc file and read the test attributes
        with open(self.desc_path) as desc_file:
            desc_lines = desc_file.read().split('\n')
            for line in desc_lines:
                if line.startswith('#'):
                    continue
                else:
                    self.attributes[line.split(':')[0]] = line.split(':')[1]
        
        # Read the key file into a 2D dictionary
        with open(self.key_path) as r:
            key_csv = csv.DictReader(r)
            # Key goes in a 2 layer dict with layer 1 indexed by uid and layer 2 indexed by results db column name
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
        # Get the job_id 
        self.job_id = json.loads(r.text)['jobid']
   
    # Check the status of the job.  Hold execution until job complete
    def checkStatus(self,sleep_time):
        while self.job_status == '':
            json_response = requests.get('%s/rest/service/status?jobid=%s' %(self.url_base, self.job_id))
            json_status = json.loads(json_response.text)['status']
            if json_status in ['Success', 'Salvaged', 'Error']:
                self.job_status = json_status
            else:
                time.sleep(sleep_time)
    
    # Private function to handle special output parsing needs of some test types.  Everything is returned as a string.
    def _data_parse(self,raw_tuple,col):
                
        if str(raw_tuple) == 'None':
            return str(raw_tuple)
        
        datapoint = raw_tuple[0]
        
        if col == 'cosmic_protein_change':
            # Just want to verify the protein change code
            return str(datapoint).split(' ')[0]
        elif col.startswith('exac'):
            # Change zeros from 0.0 to 0. Round to 4 sig figs if it's a number.
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
            
    # Verify that the entries in the key dictionary match the entries in the output SQL table
    def verify(self):
        # Result is logical pass/fail.  Initially set to pass and set to fail if a result does not match the key.
        self.result = True
        
        if self.job_status != 'Error':
            db = MySQLdb.connect(host="192.168.99.100",
                             port=3306, 
                             user="root", 
                             passwd="1", 
                             db="cravat_results")
            
            try:
                cursor = db.cursor()
                # Data is a dict that will match the key dict if test is passed
                self.data = {}
                for uid in self.key:
                    self.data[uid] = {}
                    for col in self.key[uid]:
                        query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %(col, self.job_id, uid)
                        cursor.execute(query)
                        # data_parse is needed to parse some columns
                        datapoint = self._data_parse(cursor.fetchone(),col)
                        correct = self.key[uid][col] == datapoint
                        if datapoint == () or not(correct):
                            self.result = False
                            self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %r\n\tRecieved: %r\n' %(uid, col, self.key[uid][col], datapoint)
                        self.data[uid][col] = datapoint
            except Exception:
                print traceback.format_exc()
                self.result = False
            finally:
                if self.result:
                    self.log_text = 'Passed\n'
                else:
                    self.log_text = 'Failed\n' + self.log_text
                try:
                    cursor.close()
                    db.close()
                except Exception:
                    pass
        else:
            self.data = 'Submission Failed'
            self.result = False
            self.log_text = 'Submission Failure'
        