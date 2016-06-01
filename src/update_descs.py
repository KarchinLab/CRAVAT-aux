import xml.etree.ElementTree as ET
import os

test_cases = ['all'] # Input tests to run as list of strings, or use 'all' to run every test in directory
test_cases_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases','cravat'))

if test_cases == ['all']:
    test_list = os.listdir(test_cases_dir)
    for item in test_list[:]:
        # Ignore dirs that start with #
        if item.startswith('#'):
            test_list.remove(item)
else:
    test_list = test_cases

for test in test_list:
    test_dir = os.path.join(test_cases_dir,test)
    desc_path = os.path.join(test_dir,'%s_desc.xml' %test)
    ## Things to do to desc files go here ###
    desc = ET.parse(desc_path)
    tab = ET.Element('tab')
    tab.text = 'variant'
    desc.getroot().append(tab)
    desc.write(desc_path)