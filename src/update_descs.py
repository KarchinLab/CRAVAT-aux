import xml.etree.ElementTree as ET
import os

test_cases = ['all'] # Input tests to run as list of strings, or use 'all' to run every test in directory
test_cases_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases'))

if test_cases == ['all']:
    test_list = os.listdir(test_cases_dir)
    for item in test_list[:]:
        # Ignore dirs that start with #
        if item.startswith('#'):
            test_list.remove(item)
elif test_cases == ['all_vcf']:
    test_list = os.listdir(test_cases_dir)
    for item in test_list[:]:
        # Ignore dirs that start with #
        if item.startswith('#'):
            test_list.remove(item)
        # Ignore dirs that don't include _vcf
        elif not('_vcf' in item):
            test_list.remove(item)
elif test_cases == ['all_cravat']:
    test_list = os.listdir(test_cases_dir)
    for item in test_list[:]:
        # Ignore dirs that start with #
        if item.startswith('#'):
            test_list.remove(item)
        # Ignore dirs that don't include _vcf
        elif ('_vcf' in item):
            test_list.remove(item)
else:
    test_list = test_cases

for test in test_list:
    test_dir = os.path.join(test_cases_dir,test)
    desc_path = os.path.join(test_dir,'%s_desc.xml' %test)
    tree = ET.parse(desc_path)
    tree.write(desc_path)