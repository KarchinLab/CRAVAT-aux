from TestCase import TestCase
import os
import time

main_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases'))

def print_log(log_name):
    log_file = open('C:/Users/Kyle/cravat/testing/test_cases/#logs/%s.txt' % log_name,'w')
    log_text = time.strftime('Date: %y-%m-%d\nTime: %H:%M:%S\n')
    # Log header contains info on whole test
    log_text += """Tests: %d
%r
Passed: %d
%r
Failed: %d
%r
Time: %s seconds
    """ %(len(test_list), test_list, len(results['pass']), results['pass'], len(results['fail']), results['fail'], total_time)
    
    # Log then contains details of each test
    log_text += '\nTest Details\n' + '='*80 + '\n'
    for test in tests:
        curTest = tests[test]
        log_text += '%s\n%s\n' % (curTest.name,curTest.job_id)
        if curTest.result:
            log_text += 'Passed\n'
        else:
            log_text += 'Failed\n'
            log_text += curTest.log_text
        log_text += 'Time: %s \n\n%s\n' %(curTest.elapsed_time, '-'*50)
    log_file.write(log_text)
    log_file.close()    


# Initialize with options
url = 'http://192.168.99.100:8888/CRAVAT'
test_cases = ['dbsnp'] # Input tests to run as list of strings, or use 'all' to run every test in directory

# Generate list of tests to run, either from dir names in main dir, or user input
if test_cases == ['all']:
    test_list = os.listdir(main_dir)
    for item in test_list[:]:
        if item.startswith('#'):
            test_list.remove(item)
else:
    test_list = test_cases

# Run tests. Store resulting test objects in dictionary, indexed by test name   
print 'Test Started'
total_time = 0
tests = {}
# Results will store names of tests that passed or failed, used later to summarize test
results = {'pass':[],'fail':[]}
for test in test_list:
    start_time = time.time()
    test_dir = '%s/%s' %(main_dir,test)
    print '%s\nStarting: %s' % ('-'*25,test)
    
    # Make an AtomicCase object with a temporary name
    curTest = TestCase(test_dir,url)
    curTest.getAttributes()
    curTest.getKey()
    
    curTest.submitJob()
    print 'Job Submitted: %s' % curTest.job_id
    curTest.checkStatus(1) 
    # Test will not continue until checkStatus() is complete
    print 'Submission %s: %s' % (curTest.job_status, curTest.job_id)
    # If test submission was successful, get the data and check that it matches the key
    if curTest.job_status != 'Error':
        curTest.verify()
 
    else:
        curTest.data = 'Submission Failed'
        curTest.result = False
        curTest.log_text = 'Submission Failure'
    
    # curTest.result is a logical T/F for a fully passed test.  Tests names are recorded to results dict here    
    if curTest.result:
        print 'Passed: %s' %curTest.name
        results['pass'].append(curTest.name)
    else:
        print 'Failed: %s' %curTest.name
        print curTest.log_text
        results['fail'].append(curTest.name)
    
    # curTest.elapsed_time records how long the test took to run    
    end_time = time.time()
    curTest.elapsed_time = round(end_time - start_time,2)
    print "%s seconds" % curTest.elapsed_time
    # Enter the test object into the dict   
    tests[test] = curTest
    total_time += curTest.elapsed_time

# Print log file, name of file is cur date/time
print_log(time.strftime('%y-%m-%d-%H-%M-%S'))
# Print closing comments to command line
print '%s\nPassed: %d\n%r\nFailed: %d\n%r' %('-'*25,len(results['pass']),results['pass'],len(results['fail']),results['fail'])
print '%s seconds\n%s\nTest Complete' %(total_time,'-'*25)