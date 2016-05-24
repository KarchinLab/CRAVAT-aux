import requests
import json
import time
import MySQLdb
import traceback
import math
import csv
import os
import xml.etree.ElementTree as ET

class TestCase(object):
    
    # _recurse_xml will move recursively through an xml.etree.ElementTree object and transform it into a nested dictionary.
    def _recurse_xml(self,d):
        out = {}
        if len(d):
            for k in d:
                if len(k.attrib):
                    out[k.attrib['name']] = self._recurse_xml(k)
                else:
                    out[k.tag] = self._recurse_xml(k)
        else:
            if d.text == None:
                out = ''
            else:
                out = d.text
                rep_dict = {'\\x20':'\x20'}
                for old in rep_dict:
                    out = out.replace(old,rep_dict[old])
        return out
    
    def __init__(self,path,url):
        self.path = path
        self.url_base = url
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
            self.desc = self._recurse_xml(desc_xml)
        
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
             'email': self.desc['email'],
             'analyses': self.desc['analyses']
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
        
    def _compare(self,datapoint, keypoint, method, modifier):    
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
            # Data is a dict that will match the key dict if test is passed
            self.data = {}
            
            try:
                cursor = db.cursor()
                for uid in self.key:
                    self.data[uid] = {}
                    for col in self.key[uid]:
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
        