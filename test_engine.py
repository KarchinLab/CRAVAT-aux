from TestCase import TestCase
import os
import time
import XMLConverter
import collections
import argparse


def parse_test_list(cases, main_dir):
    t_list = []
    for case in cases:
        case_args = case.split('/')
        target = case_args[0]
        try:input_codes = case_args[1].split(',')
        except: input_codes = 'all'
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
    # Put tests to run as list of strings, or use ['all'] to run every test in suite
    sys_args_parser = argparse.ArgumentParser()
    sys_args_parser.add_argument('include', 
                          help='List of cases to include.  Format as "case_name/input1,input2". Separate with ":".')
    sys_args_parser.add_argument('-ex','--exclude',
                          help='List of cases to exclude.  Format as "case_name/input1,input2". Separate with ":".')
    sys_args = sys_args_parser.parse_args()
    test_cases = sys_args.include.split(':')
    if sys_args.exclude:
        exclude_cases = sys_args.exclude.split(':')
    else:
        exclude_cases = ''
    curdir = os.path.dirname(os.path.abspath(__file__))
    test_cases_dir = os.path.normpath(os.path.join(curdir,os.path.pardir,'test_cases'))
    
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
    args = XMLConverter.xml_to_dict(os.path.join(test_cases_dir,'TestArguments.xml'))           
    for char in ['[',']','\'',' ']:
        args['expected_failures'] = args['expected_failures'].replace(char,'')
    args['expected_failures'] = args['expected_failures'].strip().split(',')
        
    # Define log filename and start log writing.
    log_dir = os.path.normpath(os.path.join(curdir,os.path.pardir,'logs'))
    log_name = time.strftime('%y-%m-%d-%H-%M-%S')
    log_text = time.strftime('Date: %y-%m-%d\nTime: %H:%M:%S\n')
    log_text += 'CRAVAT URL: %s\n' %args['url']
    
    # Results will store names of tests that passed or failed, used later to summarize test
    results = {'pass':[], 'unexp_pass':[], 'fail':[], 'unexp_fail':[]}
    
    ### Run tests ###
    tests = collections.OrderedDict() # Will store resulting test objects in order they were run  
    print 'Test Started'
    print 'CRAVAT URL: %s/n' %args['url']
    print 'Tests: %r' %test_cases
    print 'Excluding: %r' %exclude_cases
    print 'Test Dirs: %s' %', '.join(test_list)
    total_time = 0
    ######################################################################
    for test in test_list:
        start_time = time.time()
        test_dir = os.path.join(test_cases_dir, test)
        test_name = test.split('\\')[1]
        print '%s\nStarting: %s' %('-'*25, test_name)
        
        # Make a TestCase object with a temporary name. It gets stored in the tests dict at the end.
        curTest = TestCase(test_name,test_dir)
        
        # Submit job
        if curTest.desc['sub_method'] == 'post':
            curTest.submitJobPOST(args['url'], args['email'])
            print 'Job Sent via POST: %s' %curTest.job_id
            # Test will not continue until checkStatus() is complete
            curTest.checkStatus(args['url'],1) 
            print 'Submission %s' %curTest.job_status
            # Check that data matches key
            curTest.verify(args['db_info'])
            
        elif curTest.desc['sub_method'] == 'get':
            print 'Submitting lines using GET'
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
    summary = 'Tests: %d\n\n' %len(test_list)
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
        expected_fail = curTest.name in args['expected_failures']
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