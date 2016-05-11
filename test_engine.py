from TestCase import TestCase
import os
import time

print 'Test Started'
# Initialize with options
main_dir = 'C:/Users/Kyle/cravat/testing/test_cases'
url = 'http://192.168.99.100:8888/CRAVAT'
test_cases = ['all'] # Input tests to run as list of strings, or use 'all' to run every test in directory

# Generate list of tests to run, either from dir names in main dir, or user input
if test_cases == ['all']:
    test_list = os.listdir(main_dir)
    test_list.remove('#logs')
else:
    test_list = test_cases

# Run tests. Store resulting test objects in dictionary, indexed by test name   
total_time = 0
tests = {}
# Results will store names of tests that passed or failed, used later to summarize test
results = {'pass':[],'fail':[]}
for test in test_list:
    start_time = time.time()
    test_dir = '%s/%s' %(main_dir,test)
    print '%s\nStarting: %s' % ('-'*25,test)
    
    # Make a TestCase object with a temporary name
    curTest = TestCase(test_dir,url)
    curTest.getAttributes()
    curTest.getKey()
    
    curTest.submitJob()
    print 'Job Submitted: %s' % curTest.job_id
    curTest.checkStatus() 
    # Test will not continue until checkStatus() is complete
    print 'Submission %s: %s' % (curTest.job_status, curTest.job_id)
    
    # If test submission was successful, get the data and check that it matches the key
    if curTest.job_status != 'Error':
        curTest.getData()
        curTest.verify()

    else:
        curTest.data = 'Submission Failed'
        curTest.result = False
    
    # curTest.result is a logical T/F for a fully passed test.  Tests names are recorded to results dict here    
    if curTest.result:
        print 'Passed: %s' %curTest.name
        results['pass'].append(curTest.name)
    else:
        print 'Failed: %s' %curTest.name
        results['fail'].append(curTest.name)
    
    # curTest.elapsed_time records how long the test took to run    
    end_time = time.time()
    curTest.elapsed_time = round(end_time - start_time,2)
    print "%s seconds" % curTest.elapsed_time
    # Generate the curTest specific text for the log
    curTest.logText()
    # Enter the test object into the dict   
    tests[test] = curTest
    total_time += curTest.elapsed_time

# Print closing comments to command line
print '-'*25
print 'Passed: %d' %len(results['pass'])
print results['pass'] 
print 'Failed: %d' %len(results['fail'])
if len(results['fail']) != 0:
    print results['fail']    
print '%s seconds\n%s\nTest Complete' %(total_time,'-'*25,)

# Print log file, name of file is cur date/time
log_name = time.strftime('%y-%m-%d-%H-%M-%S')
log_file = open('C:/Users/Kyle/cravat/testing/test_cases/#logs/%s.txt' % log_name,'w')
log_text = time.strftime('Date: %y-%m-%d\nTime: %H:%M:%S')
# Log header contains info on whole test
log_text += """
Tests: %d
    %r
Passed: %d
    %r
Failed: %d
    %r
Time: %ss
""" %(len(test_list), test_list, len(results['pass']), results['pass'], len(results['fail']), results['fail'], total_time)

# Log then contains details of each test
log_text += '\nTest Details\n' + '='*80
for test in tests:
    curTest = tests[test]
    log_text += '\n%s\n\t%s' % (curTest.name, curTest.job_id)
    if not(curTest.result):
        log_text += '\n\tFailed'
        log_text += '\n\n\tExpected: %s\n\tRecieved: %s\n\tFailures by line:' %(curTest.key, curTest.data)
        # Failures by line are written in from each test object
        log_text += tests[test].log_text
    else:
        log_text += '\n\tPassed'
    log_text += '\n\tTime: %s \n\n%s' %(curTest.elapsed_time, '-'*50)
log_file.write(log_text)
log_file.close()