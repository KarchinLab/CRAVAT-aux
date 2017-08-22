"""
Submit -> Check -> Retrieve -> Compare

XML result? for easy comparison?
"""
from suds.client import Client
import os
import shutil
import suds
import sys
import time
import urllib
import zipfile

class Job:
    client = None
    uid = ''
    job_id = ''
    input_filename = ''
    hg18 = ''
    analysis_type = ''
    analyses = ''
    classifier = ''
    mutationbasename = ''
    
    def __init__(self, line, client, opts):
        self.opts = opts
        [self.uid, self.input_filename, self.hg18, self.analysis_type] = line.rstrip().split(' ')
        if self.hg18 == 'hg18':
            self.hg18 = 'on'
        else:
            self.hg18 = 'off'
        self.client = client
        if self.analysis_type == 'driver':
            self.analyses = 'CHASM_SnvGet'
            self.classifier = self.opts['CHASM_classifier']
        elif self.analysis_type == 'functional':
            self.analyses = 'VEST_SnvGet'
            self.classifier = None
        elif self.analysis_type == 'geneannotationonly':
            self.analyses = None
            self.classifier = ''

        self.mutationbasename = os.path.basename(self.input_filename)
                
    def submit(self):
        # Loads mutations.
        input_f = open(self.input_filename)
        mutations = ''.join(input_f.readlines())
        input_f.close()
        
        # Submits.
        try:
            self.client.service.init(classifier=self.classifier, 
                                     email=self.opts['email'],
                                     mutations=mutations,
                                     mutationbasename=self.mutationbasename,
                                     analysistype=self.analysis_type,
                                     analyses=self.analyses,
                                     tsvreport=self.opts['tsvreport'],
                                     functionalannotation=self.opts['functionalannot'],
                                     hg18=self.hg18,
                                     mupitinput=self.opts['mupitinput'])
        except suds.WebFault as detail:
            print detail
        self.job_id = self.client.service.submit()
        
        # Clear mutations.
        mutations = ''

    def getstatus(self):
        return self.client.service.finished(self.job_id)
    
    def get_errormsg(self):
        return self.client.service.getErrorMessage(self.job_id)

class TabFile:
    
    def __init__(self, filename):
        f = open(filename)
        self.lines = f.readlines()
        f.close()

def submit_check_retrieve(test_name, test_definition_filename):
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
        if line[0] == '!':
            [key, value] = line[1:].rstrip().split('=')
            opts[key] = value
        else:
            break # Parameters come before jobs.
    f.close()

    # Gets the Web Service client.    
    if opts['server'] == 'prod':
        client = Client('http://kwww.cravat.us/webapi?wsdl')
    elif opts['server'] == 'dev':
        client = Client('http://karchin-web02.icm.jhu.edu:8181/CRAVAT/webapi?wsdl')
    
    # Submits jobs.
    print 'Submitting jobs'
    jobs = {}
    f = open(test_definition_filename)
    for line in f:
        mark = line[0]
        if mark == '$':
            break
        elif line[0] != '!':
            job = Job(line, client, opts)
            job.submit()
            jobs[job.job_id] = job
            print ' ', job.uid, job.job_id
    f.close()
    
    # Checks job finish.
    print 'Checking jobs'
    jobs_to_check = [job_id for job_id in jobs.keys() if job_id.count('error') == 0]
    while len(jobs_to_check) > 0:
        for job_id in jobs_to_check:
            job = jobs[job_id]
            ret = job.getstatus()
            if ret != 0:
                if ret == 1:
                    job.state = 'success'
                    print ' ', job.uid, job_id, 'success', len(jobs_to_check), 'jobs remaining'
                elif ret == -1:
                    job.state = 'fail'
                    print ' ', job.uid, job_id, 'failed', len(jobs_to_check), 'jobs remaining'
                    print '   ', job.input_filename, job.analysis_type, job.hg18
                    print '   ', job.get_errormsg()
                jobs_to_check.remove(job.job_id)
        if len(jobs_to_check) == 0:
            break
        time.sleep(10)
    
    # Writes the log file.
    wf_test_log = open(test_log_filename, 'w')
    for job in jobs.values():
        wf_test_log.write(job.uid + '\t' + job.job_id + '\t' + job.state + '\n')
    wf_test_log.close()
    
    # Retrieves result files.
    print 'Retrieving result files'
    jobs_to_check = [job_id for job_id in jobs.keys() if job_id.count('error') == 0]
    for job_id in jobs_to_check:
        # Downloads.
        url = 'http://karchin-web02.icm.jhu.edu:8181/CRAVAT_RESULT/' + job_id + '/' + job_id + '.zip'
        print ' url=', url
        result_zip_filename =  os.path.join(test_dir, jobs[job_id].uid + '_' + job_id + '.zip')
        urllib.urlretrieve(url, result_zip_filename)
        
        # Extracts.
        z = zipfile.ZipFile(result_zip_filename)
        z.extractall(path=test_dir)
        z.close()
        
        # Renames.
        result_dir = os.path.join(test_dir, job_id)
        new_result_dir = os.path.join(test_dir, jobs[job_id].uid)
        os.rename(result_dir, new_result_dir)
        
        # Cleans up.
        os.remove(result_zip_filename)

########################
#
# PROGRAM START
#
########################

if len(sys.argv) < 3:
    print 'USAGE: python regression_test_by_web_service.py TEST_NAME TEST_DEFINITON_FILENAME'
    sys.exit(1)

test_name = sys.argv[1]
test_definition_filename = sys.argv[2]

result_basedir = 'D:\\CRAVAT\\cravat_sandbox_karchinlab\\diagnostic_tests\\result_downloads\\'

submit_check_retrieve('dev', 'test_definitions\\test_definition.txt')