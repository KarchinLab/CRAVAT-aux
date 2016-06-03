from TestCase import TestCase
import os
import time
import xml.etree.ElementTree as ET
from XML_conversions import recurse_to_dict
import collections

### Define tests to run ###
test_cases = ['all\\all'] # Input tests to run as list of strings, or use ['all'] to run every test in suite
test_cases_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases'))

# Generate list of tests to run
test_list = []
for case in test_cases:
    case_args = case.split('\\')
    target = case_args[0]
    input_codes = case_args[1].split(',')
    if target == 'all':
        target_dirs = [d for d in os.listdir(test_cases_dir) if os.path.isdir(os.path.join(test_cases_dir,d))]
    else:
        target_dirs = [target]
    for target_dir in target_dirs:
        dirs_in_target = os.listdir(os.path.join(test_cases_dir,target_dir))
        for input_dir in dirs_in_target:
            if 'all' in input_codes:
                test_list.append('%s\\%s' %(target_dir,input_dir))
            else:
                for code in input_codes:
                    if input_dir.endswith('_%s' % code):
                        test_list.append('%s\\%s' %(target_dir,input_dir))

### Perform startup tasks ###
# Read in the TestArguments file containing pointers to docker container and results database
with open(os.path.join(test_cases_dir,'TestArguments.xml'),'r') as args_file:
            args_xml = ET.parse(args_file).getroot()
args = recurse_to_dict(args_xml)
for char in ['[',']','\'',' ']:
    args['expected_failures'] = args['expected_failures'].replace(char,'')
args['expected_failures'] = args['expected_failures'].strip().split(',')
    
# Define log directory and start log writing.
log_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'logs'))
log_name = time.strftime('%y-%m-%d-%H-%M-%S')
log_text = time.strftime('Date: %y-%m-%d\nTime: %H:%M:%S\n')

# Results will store names of tests that passed or failed, used later to summarize test
results = {'pass':[],'fail':[]}

### Run tests ###
tests = collections.OrderedDict() # Will store resulting test objects in order they were run  
print 'Test Started'
total_time = 0
######################################################################
for test in test_list:
    start_time = time.time()
    test_dir = os.path.join(test_cases_dir,test)
    test_name = test.split('\\')[1]
    print '%s\nStarting: %s' %('-'*25,test_name)
    
    # Make a TestCase object with a temporary name. It gets stored in the tests dict at the end.
    curTest = TestCase(test_name,test_dir)
    
    # Submit job 
    curTest.submitJob(args['url'],args['email'])
    print 'Job Sent: %s' %curTest.job_id
    # Test will not continue until checkStatus() is complete
    curTest.checkStatus(args['url'],1) 
    print 'Submission %s: %s' %(curTest.job_status, curTest.job_id)
    
    # Check that data matches key
    curTest.verify(args['db_info'])
    
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
    print "%s seconds" %curTest.elapsed_time
    total_time += curTest.elapsed_time

    # Enter the test object into the dict   
    tests[test] = curTest  
######################################################################

results['unexp_fail'] = [t for t in results['fail'] if t not in args['expected_failures']]
results['unexp_pass'] = [t for t in args['expected_failures'] if t in results['pass']]
   
# Print closing comments to command line
print '-'*25
print 'Passed: %d\n%r' %(len(results['pass']),results['pass'])
print 'Unexpected Sucesses: %d\n%r' %(len(results['unexp_pass']), results['unexp_pass'])
print 'Failed: %d\n%r' %(len(results['fail']),results['fail'])
print 'Unexpected Failures: %d\n%r' %(len(results['unexp_fail']), results['unexp_fail'])
print '%s seconds\n%s\nTest Complete' %(total_time,'-'*25)

### Print log file ###
log_text += """Tests: %d
%r
Successes: %d
%r
Unexpected Successes: %d
%r
Failures: %d
%r
Unexpected Failures: %d
%r
Time: %s seconds
""" %(len(test_list), test_list, len(results['pass']), results['pass'], len(results['unexp_pass']), results['unexp_pass'],\
      len(results['fail']), results['fail'], len(results['unexp_fail']), results['unexp_fail'], total_time)
# Log then contains details of each test
log_text += '\nTest Details\n' + '='*80 + '\n'
for curTest in tests.values():
    log_text += '%s \n%s \n%s \n%s \nTime: %s\n\n' %('-'*25,curTest.name,curTest.job_id,curTest.log_text,curTest.elapsed_time)
    
# Write log text to file
with open(os.path.join(log_dir,'%s.txt' %log_name),'w') as log_file:
    log_file.write(log_text)