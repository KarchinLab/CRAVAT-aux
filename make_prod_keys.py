import TestCase
import os
import shutil
import argparse

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
        if os.path.isdir(item_path) and os.listdir(item_path):
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