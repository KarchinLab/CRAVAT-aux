import requests
import json
import time
import traceback
import math
import csv
import os
import MySQLdb
from XML_conversions import xml_to_dict


class TestCase(object):
    
    def __init__(self,name,path):
        self.name = name
        self.path = path
        test_dir = os.path.split(self.path)[1]
        self.input_path = os.path.join(self.path,'%s_input.txt' %test_dir)
        self.key_path = os.path.join(self.path,'%s_key.csv' %test_dir)
        self.desc_path = os.path.join(self.path, '%s_desc.xml' %test_dir)
        self.desc = {}
        self.key = {}
        self.job_id = ''
        self.job_status = ''
        self.data = {}
        self.result = False
        self.log_text = ''

        # Read test desc file to a dict
        self.desc = xml_to_dict(self.desc_path)
        
        
            
    # Submit the job to cravat   
    def submitJobPOST(self,url_base,email):
        data = {'email': email}
        for param in self.desc['sub_params']:
            data[param] = self.desc['sub_params'][param]
        files = {
                'inputfile': open(self.input_path, 'r')
                }
        r = requests.post(url_base+'/rest/service/submit', files=files, data=data)
        # Get the job_id 
        self.job_id = json.loads(r.text)['jobid']
    
    # Submit the job line-by-line using GET, store the json outputs as subdicts in the self.data dict
    def submitJobGET(self,url_base):
        self.result = True
        with open(self.input_path) as f:
            lines = f.read().split('\n')
        req_url = url_base + '/rest/service/query?mutation='
        self.data = {}
        uid = ''
        for line in lines:
            line_as_list = None
            response = None
            full_get_url = None
            line_as_list = line.split('\t')
            uid = line_as_list.pop(0)
            if len(line_as_list) > 5: line_as_list = line_as_list[:5]
            if not(line_as_list[0].startswith('chr')): line_as_list[0] = 'chr' + line_as_list[0]
            full_get_url = req_url + '_'.join(line_as_list)
            try:
                response = requests.get(full_get_url)
                self.data[uid] = json.loads(response.content)
            except:
                self.result = False
                error_text = 'Error: %s\n\tURL: %s\n\tHTTP Code: %s\n\t%s' \
                %(uid, full_get_url, response.status_code, traceback.format_exc())
                self.log_text += error_text
                print error_text
                self.data[uid] = error_text
                continue
        out_filename = '%s_%s.txt' %('json_output',time.strftime('%y-%m-%d-%H-%M-%S'))
        with open(os.path.join(self.path, out_filename),'w') as f:
            for uid in sorted(self.data):
                f.write('%s\n' %uid)
                print type(self.data[uid])
                if type(self.data[uid]) is dict:
                    for field in sorted(self.data[uid]):
                        f.write('\t%s: %s\n' %(field, self.data[uid][field]))
                elif type(self.data[uid]) is str:
                    f.write(self.data[uid])
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
        # Read the key file into a 2D dict
        with open(self.key_path) as r:
            key_csv = csv.DictReader(r)
            # Key goes in a 2 layer dict with layer 1 indexed by first column of the sql file
            # and layer 2 indexed by results db column name
            self.sql_key = key_csv.fieldnames[0]
            for row in key_csv:
                self.key[row[self.sql_key]] = row
                del self.key[row[self.sql_key]][self.sql_key]     
        # Result is logical pass/fail.  Initially set to pass and set to fail if a result does not match the key.
        self.result = True
        # Only attempt verification if job was successfully processed
        if self.job_status != 'Error':
            # Data is a dict that will store values from SQL.  May use it later
            self.data = {}
            # These will count points tried and points failed
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
                        # If key at this entry has a value, assign it to keypoint.  Otherwise, skip this entry
                        if self.key[row][col]:
                            keypoint = self.key[row][col]
                        else:
                            self.data[row][col] = self.key[row][col]
                            continue
                        
                        # If verification for this entry errors, the error will be logged and execution will continue
                        try:
                            # Run the correct query on the SQL table that the row exists in
                            for table in self.desc['tab'].strip().split(','):
                                row_found = False
                                row_count_query = 'SELECT count(*) FROM %s_%s WHERE %s = \'%s\';' \
                                                %(self.job_id, table, self.sql_key, row)
                                try:
                                    cursor.execute(row_count_query)
                                    row_count = cursor.fetchone()[0] # Will be 0 if row not found in current SQL table
                                except:
                                    row_count = 0
                                if row_count == 1:
                                    row_found = True
                                    data_query = 'SELECT %s FROM %s_%s WHERE %s = \'%s\';' \
                                        %(col, self.job_id, table, self.sql_key, row)
                                    cursor.execute(data_query)
                                    datapoint = cursor.fetchone()[0]
                                    break
                            # If row was not found in given SQL tables, throw an error
                            if not(row_found):
                                raise LookupError('Row not found')
                            
                            # If there are special verification methods listed in verify_rules and neither datapoint nor
                            # keypoint are None or Error, use the method in desc. Otherwise, default to string_exact.
                            try:
                                if keypoint != None and datapoint != None and col in self.desc['verify_rules'].keys():
                                    method = self.desc['verify_rules'][col]['method']
                                    modifier = self.desc['verify_rules'][col]['modifier']
                                else:
                                    method = 'string_exact'
                                    modifier = None
                            except:
                                    method = 'string_exact'
                                    modifier = None
                                      
                            # Use self._compare, with method assigned above, to check if datapoint matches keypoint
                            correct = self._compare(datapoint, keypoint, method, modifier)
                            if not(correct):
                                self.result = False
                                points_failed += 1
                                self.log_text += 'Variant Key: %s\n\tColumn: %s\n\tExpected: %r\n\tRecieved: %r\n\tQuery: %s\n\tMethod: %s\n\tModifier: %r\n' \
                                                %(row, col, keypoint, datapoint, data_query, method, modifier)
                           
                            self.data[row][col] = datapoint
                        # Error in SQL selection. Log exception and continue
                        except:
                            self.result = False
                            points_failed += 1
                            self.log_text += 'Variant Key: %s\n\tColumn: %s\n\tExpected: %s\n\tQuery: %s\n\tError: \n%s\n'\
                                          %(row, col, keypoint, data_query, traceback.format_exc())
            # Loop breaking error. Log exception and close.
            except:
                self.result = False
                points_failed += 1
                try:
                    self.log_text += 'Variant Key: %s\n\tColumn: %s\n\tVerification Error: \n%s\n'\
                                 %(row, col, traceback.format_exc())
                except:
                    self.log_text += traceback.format_exc()
            finally:
                # Print test summary comments to test log
                if self.result:
                    self.log_text = 'Passed\n'
                else:
                    self.log_text = 'Failed %d of %d\n' %(points_failed,points) + self.log_text
                # Close the db
                try:
                    cursor.close()
                    db.close()
                except Exception:
                    print traceback.format_exc()
                    pass
        # Go here if submission was not successful
        else:
            self.result = False
            self.data = 'Submission Failure'
            self.log_text = 'Submission Failure'
        