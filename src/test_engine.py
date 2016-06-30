from TestCase import TestCase
import os
import time
from XML_conversions import xml_to_dict
import collections


def parse_test_list(cases,main_dir):
    t_list = []
    for case in cases:
        case_args = case.split('\\')
        target = case_args[0]
        input_codes = case_args[1].split(',')
        if target == 'all':
            target_dirs = [d for d in os.listdir(main_dir) if os.path.isdir(os.path.join(main_dir,d))]
        else:
            target_dirs = [target]
        for target_dir in target_dirs:
            dirs_in_target = [d for d in os.listdir(os.path.join(main_dir,target_dir)) if os.path.isdir(os.path.join(main_dir,target_dir,d))]
            for input_dir in dirs_in_target:
                if 'all' in input_codes:
                    t_list.append('%s\\%s' %(target_dir,input_dir))
                else:
                    for code in input_codes:
                        if input_dir.endswith('_%s' % code):
                            t_list.append('%s\\%s' %(target_dir,input_dir))
    return t_list

if __name__ == '__main__':
    ### Define tests to run ###
    test_cases = ['pop_stats\\c'] # Input tests to run as list of strings, or use ['all'] to run every test in suite
    exclude_cases = [] # These test will not be run. Format same as test_cases
    test_cases_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases'))
    
    # Generate list of tests to run
    test_list = parse_test_list(test_cases,test_cases_dir)
    if exclude_cases:
        exclude_list = parse_test_list(exclude_cases,test_cases_dir)
    else:
        exclude_list = []
    for test in test_list[:]:
        if test in exclude_list:
            test_list.remove(test)
    
    ### Perform startup tasks ###
    # Read in the TestArguments file containing pointers to docker container and results database
    args = xml_to_dict(os.path.join(test_cases_dir,'TestArguments.xml'))           
    for char in ['[',']','\'',' ']:
        args['expected_failures'] = args['expected_failures'].replace(char,'')
    args['expected_failures'] = args['expected_failures'].strip().split(',')
        
    # Define log filename and start log writing.
    log_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'logs'))
    log_name = time.strftime('%y-%m-%d-%H-%M-%S')
    log_text = time.strftime('Date: %y-%m-%d\nTime: %H:%M:%S\n')
    
    # Results will store names of tests that passed or failed, used later to summarize test
    results = {'pass':[], 'unexp_pass':[], 'fail':[], 'unexp_fail':[]}
    
    ### Run tests ###
    tests = collections.OrderedDict() # Will store resulting test objects in order they were run  
    print 'Test Started'
    print 'Tests: %r' %test_cases
    print 'Excluding: %r' %exclude_cases
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
        if curTest.desc['sub_method'] == 'post':
            curTest.submitJobPOST(args['url'],args['email'])
            print 'Job Sent via POST: %s' %curTest.job_id
            # Test will not continue until checkStatus() is complete
            curTest.checkStatus(args['url'],1) 
            print 'Submission %s' %curTest.job_status
            # Check that data matches key
            curTest.verify(args['db_info'])
            
        elif curTest.desc['sub_method'] == 'get':
            print 'Submitting lines using GET'
            karchin_url = 'http://karchin-web02.icm.jhu.edu:8889/CRAVAT'
            curTest.submitJobGET(args['url'])
        
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
        tests[test] = curTest  
        
    ######################################################################
    
    ### Summarize and print test suite log file ###
    results['unexp_fail'] = [t for t in results['fail'] if t not in args['expected_failures']]
    results['unexp_pass'] = [t for t in args['expected_failures'] if t in results['pass']] 
    summary = """Tests: %d
    %r
    Passed: %d
    %r
    Unexpected Passes: %d
    %r
    Failed: %d
    %r
    Unexpected Failures: %d
    %r
    Time: %s seconds""" %(len(test_list), test_list, len(results['pass']), results['pass'], len(results['unexp_pass']), \
                          results['unexp_pass'], len(results['fail']), results['fail'], len(results['unexp_fail']), \
                          results['unexp_fail'], total_time)
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