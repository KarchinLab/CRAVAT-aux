from pyliftover import LiftOver
import os
import xml.etree.ElementTree as ET
import shutil


test_cases_dir = os.path.normpath(os.getcwd())

test_cases = ['oncogenes'] # Input tests to run as list of strings, or use 'all' to run every test in directory

if test_cases == ['all_cravat']:
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
    orig_dir = os.path.join(test_cases_dir,test)
    new_dir = orig_dir + '_hg18'
    if os.path.exists(new_dir):
        shutil.rmtree(new_dir)
        shutil.copytree(orig_dir,new_dir)
    else:
        shutil.copytree(orig_dir,new_dir)
    desc_path = os.path.join(new_dir,'%s_desc.xml' %test)
    input_path = os.path.join(new_dir,'%s_input.txt' %test)
    new_desc_path = os.path.join(new_dir,'new_desc.xml')

    desc = ET.parse(desc_path)
    a = ET.Element('hg18')
    a.text = 'on'
    desc.find('sub_params').append(a)
    ET.dump(desc)
    desc.write(new_desc_path)
    





# lo = LiftOver('hg19','hg18')
# a = lo.convert_coordinate('chr1',120506200)
# print a