import requests
import json
import time
import MySQLdb
import traceback

class TestCase(object):
    
    def __init__(self,path,url):
        self.path = path
        self.url_base = url
        self.result = False
        self.name = path.split('/')[-1]   
    
    # Get the attributes from the properly formatted description file    
    def getAttributes(self):
        self.desc_file = open('%s/%s_desc.txt'%(self.path, self.name))
        self.desc_text = self.desc_file.read().split('\n')
        self.columns = self.desc_text[0].split(':')[1].split(',')
        self.mut_format = self.desc_text[1].split(':')[1]
        self.analyses = self.desc_text[2].split(':')[1]
        self.input_path = '%s/%s_input.txt' %(self.path, self.name)
        self.key_path = '%s/%s_key.txt' %(self.path, self.name)
        self.input_text = open(self.input_path).read()
    
    # Submit the job to cravat    
    def submitJob(self):        
        data = {
             'email':'kmoad@insilico.us.com',
             'analyses': self.analyses
            }
        files = {
                'inputfile': open(self.input_path, 'r')
                }
        r = requests.post(self.url_base+'/rest/service/submit', files=files, data=data)
        self.job_id = json.loads(r.text)['jobid']
   
    # Check the status of the job.  Hold execution until job complete
    def checkStatus(self):
        self.job_status = ''
        while self.job_status == '':
            json_response = requests.get('%s/rest/service/status?jobid=%s' %(self.url_base, self.job_id))
            json_status = json.loads(json_response.text)['status']
            if json_status in ['Success', 'Salvaged', 'Error']:
                self.job_status = json_status
            
            time.sleep(1)
    
    # Read in key file  ***Needs to be more robust. plural columns, non coding mutations***
    def getKey(self):
        r = open(self.key_path)
        t = r.read().split('\n')
        out = []
        for rl in t:
            temp = rl.split(',')
            temp[0] = long(temp[0])
            out.append(tuple(temp))
        self.key = tuple(out)        
    
    # Read results from Database.
    def getData(self):
        db = MySQLdb.connect(host="192.168.99.100",
                         port=3306, 
                         user="root", 
                         passwd="1", 
                         db="cravat_results")
        self.query = 'SELECT input_line_number,%s FROM %s_variant ORDER BY input_line_number' %(','.join(self.columns), self.job_id)
        try:
            cursor = db.cursor()
            cursor.execute(self.query)
            self.data = cursor.fetchall()
        except Exception:
            print traceback.format_exc()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
    
    # Compare key to data and either verify success or find errors.
    def verify(self):
        
        # If the test was totally succesful, self.result is True
        self.result = self.data == self.key
        
        # Input lines from the input file are read in as a list
        input_lines = self.input_text.split('\n')
        if 'VCF' in input_lines[0]:
            del input_lines[0:2]
        
        # Loop through entries in key file looking for failures     
        self.lines = {}
        for key_line in self.key:
            # Since variants are sometimes missing from the output data, key list index and data index are not always the same
            key_ind = self.key.index(key_line)
            # Variant.name is 1 indexed because that is how CRAVAT assigns line numbers to the input file
            curVariant = Variant(key_ind+1)
            curVariant.keypoint = self.key[key_ind][1]
            curVariant.input = input_lines[key_ind]
            # Check to see if line is fully correct
            line_correct = key_line in self.data 
            curVariant.result = line_correct
            # If line is not fully correct, check if line is present in data, then check if data matches key
            if line_correct:
                curVariant.status = 'Success'
                # Correct data index is found by searching for input line number
                data_ind = [key_line[0] in data_line for data_line in self.data].index(True)
                curVariant.datapoint = self.data[data_ind][1]
            else:
                line_present = any([key_line[0] in data_line for data_line in self.data])
                if line_present:
                    curVariant.status = 'Incorrect'
                    data_ind = [key_line[0] in data_line for data_line in self.data].index(True)
                    curVariant.datapoint = self.data[data_ind][1] 
                else:
                    curVariant.status = 'Absent'
                    curVariant.datapoint = 'N/A'
                    
            self.lines[key_ind] = curVariant
        
    # Write he line by line failures 
    def logText(self):
        self.log_text = ''  
        for ind in self.lines:
            if not(self.lines[ind].result):
                self.log_text += """\n\t\tLine %s: 
\t\t\t%s
\t\t\tStatus: %s
\t\t\tExpected: %s
\t\t\tReceived: %s""" %(self.lines[ind].num, self.lines[ind].input, self.lines[ind].status, self.lines[ind].keypoint, self.lines[ind].datapoint)
        
# Variant class is used in the verify method of curTest to check for failures of specific variants    
class Variant(TestCase):
    def __init__(self,line_num):
        self.num = line_num        