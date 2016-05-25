import requests
import json
import time
import traceback
import math
import csv
import os
import xml.etree.ElementTree as ET
import XML_conversions
import MySQLdb


class TestCase(object):
    
    def __init__(self,path):
        self.path = path
        self.name = os.path.basename(self.path)
        self.input_path = os.path.join(self.path,'%s_input.txt' %self.name)
        self.key_path = os.path.join(self.path,'%s_key.csv' %self.name)
        self.desc_path = os.path.join(self.path, '%s_desc.xml' %self.name)
        self.desc = {}
        self.key = {}
        self.job_id = ''
        self.job_status = ''
        self.data = {}
        self.result = False
        self.log_text = ''

        # Open up the desc file and read the test attributes
        with open(self.desc_path) as desc_file:
            desc_xml = ET.parse(desc_file).getroot()
            self.desc = XML_conversions.recurse_to_dict(desc_xml)
        
        # Read the key file into a 2D dictionary
        with open(self.key_path) as r:
            key_csv = csv.DictReader(r)
            # Key goes in a 2 layer dict with layer 1 indexed by uid and layer 2 indexed by results db column name
            for row in key_csv:
                self.key[row['uid']] = row
                del self.key[row['uid']]['uid']     
            
    # Submit the job to cravat   
    def submitJob(self,url_base,email):
        data = {
                'email': email,
                'analyses': self.desc['analyses']
                }
        files = {
                'inputfile': open(self.input_path, 'r')
                }
        r = requests.post(url_base+'/rest/service/submit', files=files, data=data)
        # Get the job_id 
        self.job_id = json.loads(r.text)['jobid']
   
    # Check the status of the job.  Hold execution until job complete
    def checkStatus(self,url_base,sleep_time):
        while self.job_status == '':
            json_response = requests.get('%s/rest/service/status?jobid=%s' %(url_base, self.job_id))
            json_status = json.loads(json_response.text)['status']
            if json_status in ['Success', 'Salvaged', 'Error']:
                self.job_status = json_status
            else:
                time.sleep(sleep_time)
        
    def _compare(self,datapoint, keypoint, method, modifier):    
        # Exact string comparison
        if method == 'string_exact':
            datapoint = str(datapoint)
            keypoint = str(keypoint)
            out = datapoint == keypoint
        # Key string is first <modifier> characters
        elif method == 'string_truncate':
            modifier = int(modifier)
            datapoint = str(datapoint)
            keypoint = str(keypoint)
            out = datapoint[:modifier] == keypoint[:modifier]
        # Key string is data string up to <modifier> character
        elif method == 'string_parse':
            modifier = str(modifier)
            datapoint = str(datapoint)
            keypoint = str(keypoint)
            out = keypoint == datapoint.split(modifier)[0] 
        # Key string is present in data string. Multiple unique strings in key can be separated with <modifier> character
        elif method == 'string_included':
            modifier = str(modifier)
            datapoint = str(datapoint)
            keypoint_list = str(keypoint).split(modifier)
            out = True
            for key_string in keypoint_list:
                correct = key_string in datapoint
                if not(correct):
                    out = False
        # Key and data match when rounded to <modifier> sigfigs, will ignore preceding zeros in decimal
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
        # Key and data match when rounded to <modifier> decimal points
        elif method == 'float_round':
            datapoint = float(datapoint)
            keypoint = float(keypoint)
            modifier = int(modifier)
            data_rounded = round(datapoint,modifier)
            key_rounded = out = round(keypoint,modifier)
            out = data_rounded == key_rounded
        # Key and data match when truncated at the <modifier> decimal point
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
        # Data is within <modifier> integers of key, inclusive
        elif method == 'float_numeric_range':
            datapoint = float(datapoint)
            keypoint = float(keypoint)
            modifier = float(modifier)
            diff = abs(datapoint - keypoint)
            out = diff <= modifier
        # Data is within <modifier> percentage range of key
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
            
    # Verify that the entries in the key dictionary match the entries in the output SQL table
    def verify(self,db_args):
        # Result is logical pass/fail.  Initially set to pass and set to fail if a result does not match the key.
        self.result = True
        
        if self.job_status != 'Error':
           
            # Data is a dict that will match the key dict if test is passed
            self.data = {}
            
            try:
                db = MySQLdb.connect(host = db_args['host'],
                     port = int(db_args['port']),
                     user = db_args['user'],
                     passwd = db_args['password'],
                     db = db_args['db']
                     )
                points = 0
                points_failed = 0
                cursor = db.cursor()
                for uid in self.key:
                    self.data[uid] = {}
                    for col in self.key[uid]:
                        points += 1
                        query = 'SELECT %s FROM %s_variant WHERE uid = \'%s\';' %(col, self.job_id, uid)
                        cursor.execute(query)
                        # 
                        datapoint = cursor.fetchone()[0]
                        keypoint = self.key[uid][col]
                        #
                        if type(self.desc['verify_rules']) is dict:
                            if keypoint == None or datapoint == None or datapoint == ():
                                method = 'string_exact'
                                modifier = None 
                            elif col in self.desc['verify_rules'].keys():
                                method = self.desc['verify_rules'][col]['method']
                                modifier = self.desc['verify_rules'][col]['modifier']
                            else:
                                method = 'string_exact'
                                modifier = None 
                        else:
                            method = 'string_exact'
                            modifier = None
                        #
                        correct = self._compare(datapoint, keypoint, method, modifier)
                        if not(correct):
                            points_failed += 1
                            self.result = False
                            self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %r\n\tRecieved: %r\n\tMethod: %s\n\tModifier: %r\n' \
                                                %(uid, col, keypoint, datapoint, method, modifier)
                        self.data[uid][col] = datapoint
            except Exception:
                print traceback.format_exc()
                self.result = False
            finally:
                if self.result:
                    self.log_text = 'Passed\n'
                else:
                    self.log_text = 'Failed %d of %d\n' %(points_failed,points) + self.log_text
                try:
                    cursor.close()
                    db.close()
                except Exception:
                    pass
        else:
            self.data = 'Submission Failed'
            self.result = False
            self.log_text = 'Submission Failure'
        