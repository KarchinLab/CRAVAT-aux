from TestCase import TestCase
import os
import time
import collections
import argparse
import MySQLdb

class ConfReader(object):
    def __init__(self, path):
        self.__dict__ = {}
        with open(path) as f:
            for l in f:
                if l.strip().startswith('#'): 
                    continue
                k, v = l.strip().split('=')
                self.__dict__[k] = v
    def __getitem__(self, k):
        return self.__dict__[k]
    def __str__(self):
        return str(self.__dict__)

if __name__ == '__main__':
    ### Define test environment
    #
    #
    #
    fdir = os.path.dirname(os.path.abspath(__file__))
    test_cases_dir = os.path.join(fdir,'test_cases')
    valid_cases = []
    for item_name in os.listdir(test_cases_dir):
        item_path = os.path.join(test_cases_dir, item_name)
        if os.path.isdir(item_path):
            valid_cases.append(item_name)
    sys_args_parser = argparse.ArgumentParser()
    sys_args_parser.add_argument('-i', '--include', 
                          help='List of cases to include. Comma seperated.')
    sys_args_parser.add_argument('-e','--exclude',
                          help='List of cases to exclude.  Comma seperated.')
    sys_args = sys_args_parser.parse_args()
    
    ### Define tests to run
    #
    #
    #
    if sys_args.include:
        include_cases = sys_args.include.split(',')
        invalid_cases = set(include_cases) - set(valid_cases)
        if invalid_cases: raise Exception('Invalid cases:%s' %', '.join(invalid_cases))
    else:
        include_cases = valid_cases
    if sys_args.exclude:
        exclude_cases = sys_args.exclude.split(',')
    else:
        exclude_cases = []
    test_cases = list(set(include_cases) - set(exclude_cases))
    
    ### Perform startup tasks
    #
    #
    #
    conf = ConfReader('test_engine.conf')
    dbconn = MySQLdb.connect(host=conf['dbhost'],
                             port=int(conf['dbport']),
                             user=conf['dbuser'],
                             passwd=conf['dbpasswd'],
                             db='cravat_results')
    expected_failures = conf['expected_failures'].split(',')
    
    # Define log filename and start log writing.
    log_dir = os.path.join(fdir, 'logs')
    log_name = time.strftime('%y-%m-%d-%H-%M-%S')
    log_text = time.strftime('Date: %y-%m-%d\nTime: %H:%M:%S\n')
    log_text += 'CRAVAT URL: %s\n' %conf['url']
    
    # Results will store names of tests that passed or failed, used later to summarize test
    results = {'pass':[], 'unexp_pass':[], 'fail':[], 'unexp_fail':[]}
    
    ### Run tests ###
    #
    #
    #
    tests = collections.OrderedDict() # Will store resulting test objects in order they were run  
    print 'Test Started'
    print 'CRAVAT URL: %s' %conf['url']
    print 'Tests: %r' %', '.join(test_cases)
    total_time = 0
    for test_name in test_cases:
        start_time = time.time()
        test_dir = os.path.join(test_cases_dir, test_name)
        print '%s\nStarting: %s' %('-'*25, test_name)
        
        # Make a TestCase object with a temporary name. It gets stored in the tests dict at the end.
        curTest = TestCase(test_name,test_dir)
        curTest.verify(dbconn)
        continue
        
        # Submit job
        if curTest.desc['sub_method'] == 'post':
            curTest.submitJobPOST(conf['url'], conf['email'])
            print 'Job Sent via POST: %s' %curTest.job_id
            # Test will not continue until checkStatus() is complete
            curTest.checkStatus(conf['url'],1) 
            print 'Submission %s' %curTest.job_status
            # Check that data matches key
            curTest.verify(dbconn)
        elif curTest.desc['sub_method'] == 'get':
            print 'Submitting lines using GET'
            curTest.submitJobGET(conf['url'])
        else:
            print 'Error: Unknown job submission method.'
         
        # curTest.result is a logical T/F for a fully passed test.  Tests names are recorded to results dict here    
        if curTest.result:
            print 'Passed: %s' %curTest.name
            results['pass'].append(curTest.name)
        else:
            print 'Failed: %s' %curTest.name
            print curTest.log_text
            results['fail'].append(curTest.name)
            
        # curTest.elapsed_time records how long the test took to run    
        curTest.elapsed_time = round(time.time() - start_time,2)
        total_time += curTest.elapsed_time
        print "%s seconds" %curTest.elapsed_time
        # Enter the test object into the dict   
        tests[test_name] = curTest  
        
    ### Summarize and print test suite log file ###
    #
    #
    #
    summary = 'Tests: %d\n\n' %len(test_cases)
    col_start = 10
    col_space = 10
    result_categories = ['UF','US','EF','ES']
    summary += 'TEST'.ljust(col_start)
    for h in result_categories:
        summary += h.rjust(col_space)
    summary += '\n'
    for test in tests:
        curTest = tests[test]
        success = curTest.result
        expected_fail = curTest.name in expected_failures
        if not(success) and not(expected_fail):
            dist_in = col_start + (result_categories.index('UF') + 1) * col_space - len(curTest.name)
        elif success and expected_fail:
            dist_in = col_start + (result_categories.index('US') + 1) * col_space - len(curTest.name)
        elif not(success) and expected_fail:
            dist_in = col_start + (result_categories.index('EF') + 1) * col_space - len(curTest.name)
        elif success and not(expected_fail):
            dist_in = col_start + (result_categories.index('ES') + 1) * col_space - len(curTest.name)
        else:
            dist_in = 0
        
        summary += curTest.name
        summary += 'x'.rjust(dist_in)
        summary += '\n'
            
    summary += '\nTotal Time: %s' %total_time
    # Print summary
    print '-'*25
    print summary
    print '-'*25
    print 'Test Complete'
    
    # Print log file
    log_text += summary
    # Log then contains details of each test
    log_text += '\n\nTest Details\n' + '='*80 + '\n'
    for curTest in tests.values():
        log_text += '%s \n%s \n%s \n%s \nTime: %s\n\n' \
        %('-'*25, curTest.name, curTest.job_id, curTest.log_text, curTest.elapsed_time)
        
    with open(os.path.join(log_dir,'%s.txt' %log_name),'w') as log_file:
        log_file.write(log_text)