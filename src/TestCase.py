import requests
import json
import time
import traceback
import math
import csv
import os
import xml.etree.ElementTree as ET
import MySQLdb
from XML_conversions import recurse_to_dict


class TestCase(object):
    
    def __init__(self,name,path):
        self.name = name
        self.path = path
        bottom_dir = os.path.split(self.path)[1]
        self.input_path = os.path.join(self.path,'%s_input.txt' %bottom_dir)
        self.key_path = os.path.join(self.path,'%s_key.csv' %bottom_dir)
        self.desc_path = os.path.join(self.path, '%s_desc.xml' %bottom_dir)
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
            self.desc = recurse_to_dict(desc_xml)
        
        # Read the key file into a 2D dictionary
        with open(self.key_path) as r:
            key_csv = csv.DictReader(r)
            # Key goes in a 2 layer dict with layer 1 indexed by first column of the sql file
            # and layer 2 indexed by results db column name
            self.sql_key = key_csv.fieldnames[0]
            for row in key_csv:
                self.key[row[self.sql_key]] = row
                del self.key[row[self.sql_key]][self.sql_key]     
            
    # Submit the job to cravat   
    def submitJob(self,url_base,email):
        data = {'email': email}
        for param in self.desc['sub_params']:
            data[param] = self.desc['sub_params'][param]
        files = {
                'inputfile': open(self.input_path, 'r')
                }
        r = requests.post(url_base+'/rest/service/submit', files=files, data=data)
        # Get the job_id 
        self.job_id = json.loads(r.text)['jobid']
   
    # Check the status of the job.  Hold execution until job complete
    def checkStatus(self,url_base,sleep_time):
        while self.job_status == '':
            try:
                json_response = requests.get('%s/rest/service/status?jobid=%s' %(url_base, self.job_id))
                json_status = json.loads(json_response.text)['status']
            except:
                print traceback.format_exc()
                time.sleep(sleep_time)
                continue
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
        # True if first <modifier> characters in data string equal key string
        elif method == 'string_truncate':
            modifier = int(modifier)
            datapoint = str(datapoint)
            keypoint = str(keypoint)
            out = datapoint[:modifier] == keypoint[:modifier]
        # True if data string up to <modifier> character is equal to key string
        elif method == 'string_parse':
            modifier = str(modifier)
            datapoint = str(datapoint)
            keypoint = str(keypoint)
            out = keypoint == datapoint.split(modifier)[0] 
        # True if key string is present in data string. Multiple unique strings in key can be separated with <modifier> character
        elif method == 'string_included':
            modifier = str(modifier)
            datapoint = str(datapoint)
            keypoint_list = str(keypoint).split(modifier)
            out = True
            for key_string in keypoint_list:
                correct = key_string in datapoint
                if not(correct):
                    out = False
        # True if key and data match when rounded to <modifier> decimal points
        elif method == 'float_round':
            datapoint = float(datapoint)
            keypoint = float(keypoint)
            modifier = int(modifier)
            data_rounded = round(datapoint,modifier)
            key_rounded = out = round(keypoint,modifier)
            out = data_rounded == key_rounded
        # True if key and data match when rounded to <modifier> sigfigs. Will ignore preceding zeros in decimals.
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
        # True if key and data match when truncated at the <modifier> decimal point
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
        # True if data is within <modifier> integers of key. Inclusive
        elif method == 'float_numeric_range':
            datapoint = float(datapoint)
            keypoint = float(keypoint)
            modifier = float(modifier)
            diff = abs(datapoint - keypoint)
            out = diff <= modifier
        # True if data is within <modifier> percentage range of key. Inclusive
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
            points = 0
            points_failed = 0
            
            try:
                db = MySQLdb.connect(host = db_args['host'],
                     port = int(db_args['port']),
                     user = db_args['user'],
                     passwd = db_args['password'],
                     db = db_args['db']
                     )
                cursor = db.cursor()
                # Loop through the key dictionary, row is used as primary index, SQL col name as secondary index
                for row in self.key:
                    self.data[row] = {}
                    for col in self.key[row]:
                        points += 1
                        keypoint = self.key[row][col]
                        try:
                            for table in self.desc['tab'].split(','):
                                try:
                                    query = 'SELECT %s FROM %s_%s WHERE %s = \'%s\';' \
                                          %(col, self.job_id, table, self.sql_key, row)
                                    cursor.execute(query)
                                    datatuple = cursor.fetchone()
                                except:
                                    datatuple = None
                                if type(datatuple)==tuple: 
                                    datapoint = datatuple[0]
                                    break
                            try:
                                datapoint
                            except:
                                datapoint = 'Error: SQL entry not found.'
                            
                            # If there are special verification methods listed in verify_rules, assign those methods for their columns.
                            # Default to string_exact
                            if type(self.desc['verify_rules']) is dict:
                                if keypoint == None or datapoint == None or datapoint == () or 'Error' in str(datapoint):
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
                            # Use self.__compare, with method assigned above, to check if datapoint matches keypoint
                            correct = self._compare(datapoint, keypoint, method, modifier)
                            if not(correct):
                                points_failed += 1
                                self.result = False
                                self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %r\n\tRecieved: %r\n\tMethod: %s\n\tModifier: %r\n' \
                                                    %(row, col, keypoint, datapoint, method, modifier)
                            self.data[row][col] = datapoint
                        except:
                            print traceback.format_exc()
                            self.result = False
                            self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tError: \n%s\n'\
                                             %(row,col,keypoint,traceback.format_exc())
            except:
                self.result = False
                points_failed += 1
                self.log_text += 'Variant UID: %s\n\tColumn: %s\n\tExpected: %s\n\tError: \n%s\n'\
                                    %(row,col,keypoint,traceback.format_exc())
                print traceback.format_exc()
            finally:
                if self.result:
                    self.log_text = 'Passed\n'
                else:
                    self.log_text = 'Failed %d of %d\n' %(points_failed,points) + self.log_text
                
                try:
                    cursor.close()
                    db.close()
                except Exception:
                    print traceback.format_exc()
                    pass
        
        else:
            self.data = 'Submission Failed'
            self.result = False
            self.log_text = 'Submission Failure'
        