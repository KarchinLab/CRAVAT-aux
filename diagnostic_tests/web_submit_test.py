from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
import os
import shutil
from suds.client import Client
import sys
import time
import urllib
import zipfile
import requests
import json

def get_time_string ():
    [year, mon, day, hour, min, sec, wday, yday, isdst] = time.localtime()
    time_str = str(year) + str(mon).rjust(2, '0') + str(day).rjust(2, '0') + '_' + \
               str(hour).rjust(2, '0') + str(min).rjust(2, '0') + str(sec).rjust(2, '0')
    return time_str

class Job:
    client = None
    uid = ''
    job_id = ''
    input_filename = ''
    hg18 = ''
    analyses = ''
    classifier = ''
    mutationbasename = ''
    status = ''
    errormsg = None
    
    def __init__(self, line, driver, opts):         # I think self. just triggers that for that instance that variable means what has been put in
                                                    # This __init__ def makes global variables for the instance of the class I think
                                                    # This does allow for more than one instance of the same thing going
        self.opts = opts
        [self.uid, self.input_filename, self.hg18] = line.rstrip().split(' ')       #self.input_filename right here assumes that you are already in the directory CRAVAT/diagnostic_tests/ 
        if self.hg18 == 'hg18':
            self.hg18 = 'on'
        else:
            self.hg18 = 'off'
        self.driver = driver
        if self.opts['analysis_type'] == 'all':
            self.analyses = 'VEST;CHASM;SnvGet'
            self.classifier = self.opts['CHASM_classifier']
        elif self.opts['analysis_type'] == 'chasm':
            self.analyses = 'CHASM;SnvGet; geneannotation'
            self.classifier = self.opts['CHASM_classifier']
        elif self.opts['analysis_type'] == 'vest':
            self.analyses = 'VEST;SnvGet;geneannotation'
            self.classifier = self.opts['CHASM_classifier']
        elif self.opts['analysis_type'] == 'geneannotationonly':
            self.analyses = None
            self.classifier = ''
        elif self.opts['analysis_type'] == 'none':
            self.analyses = None
            self.classifier = ''
        self.mutation_filename = os.path.join(self.opts['diagnostic_dir'], 'input_file', self.input_filename)
        self.mutationbasename = os.path.basename(self.mutation_filename)
        if opts['server'] == 'prod':
            #self.base_url = 'http://www.cravat.us'
            self.base_url = 'http://karchin-web03.icm.jhu.edu:8080/CRAVAT'
        elif opts['server'] == 'stg':
            self.base_url = 'http://training.cravat.us'
        elif opts['server'] == 'dev':
            if self.opts['test_user'] == 'rick':
                self.base_url = 'http://dev.cravat.us' # Rick's dev server
            elif self.opts['test_user'] == 'derek':
                self.base_url = 'http://dev2.cravat.us' # Derek's dev2 server
        self.result_base_url = self.base_url + '/results'
                
    def submit(self):
        driver = self.driver
        driver.get(self.base_url)
        driver.find_element_by_id("inputfile").send_keys(self.mutation_filename)
        time.sleep(1)
        
        if self.opts['server'] in ['dev', 'stg', 'prod']:
            if self.opts['analysis_type'] == 'all':
                #driver.find_element_by_id('vestcheckbox').click()           #    ONLY CLICKING THIS NOW, BECAUSE VEST IS WRONG. TAKE THIS AWAY LATER!!!
                driver.find_element_by_id('chasmcheckbox').click()
                driver.find_element_by_id('snvgetcheckbox').click()   
                Select(driver.find_element_by_id("chasmclassifier")).select_by_visible_text(self.classifier)
            elif self.opts['analysis_type'] == 'chasm':
                driver.find_element_by_id('vestcheckbox').click()           #    ONLY CLICKING THIS NOW, BECAUSE VEST IS WRONG. TAKE THIS AWAY LATER!!
                driver.find_element_by_id('chasmcheckbox').click()
                driver.find_element_by_id('snvgetcheckbox').click()   
                Select(driver.find_element_by_id("chasmclassifier")).select_by_visible_text(self.classifier)
            elif self.opts['analysis_type'] == 'vest':
                driver.find_element_by_id('snvgetcheckbox').click()   
                time.sleep(1)
            elif self.opts['analysis_type'] == 'none':
                driver.find_element_by_id('vestcheckbox').click()
                driver.find_element_by_id('geneannotation').click()
                time.sleep(1)
        
        if self.hg18 == 'on':
            driver.find_element_by_id('hg18').click()
        driver.find_element_by_id("email").click()
        driver.find_element_by_id("email").clear()
        driver.find_element_by_id("email").send_keys(self.opts['email'])
        driver.find_element_by_name("tsvreport").click()
        driver.find_element_by_id("submit_button").click()
        time.sleep(10)
        self.job_id = None
        submit_result_text = driver.find_element_by_id('submitresultdiv').text
        if submit_result_text.count('Submission complete') > 0:
            self.job_id = submit_result_text.split('(')[1].split(')')[0][7:]
            if self.opts['server'] == 'dev':
                self.result_url = 'http://dev.cravat.us/results' + '/' + self.job_id + '/' + self.job_id + '.zip'           #THis is set up for dev.cravat.us NOT!!! dev2.cravat.us
            elif self.opts['server'] == 'stg':
                self.result_url = 'http://training.cravat.us/results' + '/' + self.job_id + '/' + self.job_id + '.zip'
            else:
                self.result_url = 'http://www.cravat.us/results'+ '/' + self.job_id + '/' + self.job_id + '.zip' 

    def findStatus(self):
        r = requests.get(self.base_url + '/rest/service/status', params={'jobid':self.job_id})
        self.status = json.loads(r.text)['status']
    
    def get_errormsg(self):
        r = requests.get(self.base_url + '/rest/service/status', params={'jobid':self.job_id})
        self.errormsg = json.loads(r.text)['errormsg']
        return self.errormsg

class TabFile:
    
    def __init__(self, filename):
        f = open(filename)
        self.lines = f.readlines()
        f.close()

def submit_check_retrieve(test_user, test_name, server, test_definition_filename):
    if test_user == 'rick':
        diagnostic_dir = 'c:\\git\\cravat\\diagnostic_tests'
        email = 'rkim@insilico.us.com'
    elif test_user == 'derek':
        diagnostic_dir = '/Users/derekgygax/Desktop/CRAVAT_MuPIT/CRAVAT/diagnostic_tests'
        email = 'dgygax@insilico.us.com'
    result_basedir = os.path.join(diagnostic_dir, 'result_downloads')
    test_definition_filename = os.path.join(diagnostic_dir, 'test_definitions', test_definition_filename)

    # Creates test result folder.
    test_dir = os.path.join(result_basedir, test_name)
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.mkdir(test_dir)
    
    # Creates log file.
    test_log_filename = os.path.join(test_dir, 'log.txt')
    
    # Reads the test definition file parameters.
    opts = {}
    f = open(test_definition_filename)
    for line in f:
        if line[0] == '#':
            continue
        elif line[0] == '!':
            [key, value] = line[1:].rstrip().split('=')
            opts[key] = value
        else:
            break # Parameters come before jobs.
    f.close()
    opts['email'] = email
    opts['diagnostic_dir'] = diagnostic_dir
    opts['result_basedir'] = result_basedir
    opts['result_dir'] = test_dir
    opts['test_user'] = test_user
    opts['server'] = server

    # Submits jobs.
    print 'Submitting jobs'
    #driver = webdriver.Firefox()
    #profile = webdriver.FirefoxProfile()
    driver = webdriver.Chrome()
    #profile = webdriver.ChromeOptions()
    driver.implicitly_wait(30000)
    #profile.set_preference("http.response.timeout", 3000)
    #profile.set_preference("dom.max_script_run_time", 3000)
    jobs = {}
    f = open(test_definition_filename)
    for line in f:
        line = line.rstrip()
        
        if line == '':
            continue
        
        mark = line[0]
        # Skips remark lines.
        if mark == '#':
            continue
        # Detects the STOP line.
        if mark == '$':
            break
        elif line[0] != '!':
            job = Job(line, driver, opts)           #line right here assumes that you are already in the directory CRAVAT/diagnostic_tests/  KNOW THAT!!
                                                    #This first line just triggers an instance, and inputs parameters. It does not do a def within Job()
            
            job.submit()
            jobs[job.job_id] = job
            print ' ', job.uid, job.job_id
    driver.close()
    
    # Checks job finish.
    print 'Checking jobs'
    jobs_to_check = jobs.keys()
    while len(jobs_to_check) > 0:
        for job_id in jobs_to_check:
            job = jobs[job_id]
            job.findStatus()
            if job.status == 'Success' or job.status == 'Salvaged':
                print ' ', job.uid, job_id, job.status, len(jobs_to_check), 'jobs remaining'
                jobs_to_check.remove(job.job_id)
            if job.status == 'Error':
                print ' ', job.uid, job_id, job.status, len(jobs_to_check), 'jobs remaining'
                print '   ', job.input_filename, job.opts['analysis_type'], job.hg18
                print '   ', job.get_errormsg()
                jobs_to_check.remove(job.job_id)
        if len(jobs_to_check) == 0:
            break
        time.sleep(10)
    
    # Writes the log file.
    wf_test_log = open(test_log_filename, 'w')
    for job in jobs.values():
        wf_test_log.write(job.uid + '\t' + job.job_id + '\t' + job.status + '\n')
    wf_test_log.close()
    
    # Retrieves result files.
    print 'Retrieving result files'
    jobs_to_check = [job.job_id for job in jobs.values() if job.status == 'Success']
    for job_id in jobs_to_check:
        # Downloads.
        job = jobs[job_id]
        result_zip_filename =  os.path.join(test_dir, jobs[job_id].uid + '_' + job_id + '.zip')
        print 'job.result_url = '  + str(job.result_url)
        urllib.urlretrieve(job.result_url, result_zip_filename)
        
        # Extracts.
        print result_zip_filename
        z = zipfile.ZipFile(result_zip_filename)
        z.extractall(path=test_dir)
        z.close()
        
        # Renames.
        result_dir = os.path.join(test_dir, job_id)
        new_result_dir = os.path.join(test_dir, jobs[job_id].uid)
        os.rename(result_dir, new_result_dir)
        
        # Cleans up.
        os.remove(result_zip_filename)

    print 'Done'

########################
#
# PROGRAM START
#
########################

if len(sys.argv) != 5:
    print 'user, testname, server, definition file name'
    sys.exit()

test_user = sys.argv[1]
test_name = sys.argv[2]
server = sys.argv[3]
definition_filepath = sys.argv[4]
submit_check_retrieve(test_user, \
                      test_name, \
                      server, \
                      definition_filepath)
