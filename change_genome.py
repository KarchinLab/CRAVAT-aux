from pyliftover import LiftOver
import os
import xml.etree.ElementTree as ET
import shutil


test_cases_dir = os.path.join(os.getcwd(),'test_cases')

test_cases = ['all_cravat'] # Input tests to run as list of strings, or use 'all' to run every test in directory

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
    print test
    orig_dir = os.path.join(test_cases_dir,test)
    
    # Make new directory. Move files to new dir with updated names . All changes will be done here
    print '\tMaking folder and files'
    new_dir = orig_dir + '_hg18'
    if os.path.exists(new_dir):
        shutil.rmtree(new_dir)
        os.mkdir(new_dir)
    else:
        os.mkdir(new_dir)
    shutil.copy(os.path.join(orig_dir,'%s_desc.xml' %test), os.path.join(new_dir,'%s_hg18_desc.xml' %test))
    shutil.copy(os.path.join(orig_dir,'%s_input.txt' %test), os.path.join(new_dir,'%s_hg18_input.txt' %test)) 
    shutil.copy(os.path.join(orig_dir,'%s_key.csv' %test), os.path.join(new_dir,'%s_hg18_key.csv' %test))    
    
    # Add a <hg18>on</hg18> tag to the desc.xml
    print '\tChanging desc file'
    desc_path = os.path.join(new_dir,'%s_hg18_desc.xml' %test)
    desc = ET.parse(desc_path)
    hg18 = ET.Element('hg18')
    hg18.text = 'on'
    desc.find('sub_params').append(hg18)
    desc.write(desc_path)
    
    # Shift genomic coordinates to hg18
    print '\tLifting over coordinates'
    input_path = os.path.join(new_dir,'%s_hg18_input.txt' %test)
    input_text = open(input_path,'r').read()
    lo = LiftOver('hg19','hg18')
    lines19 = input_text.split('\n')
    lines18 = []
    for line19 in lines19:
        elems19 = line19.split('\t')
        elems18 = elems19
        genom18 = lo.convert_coordinate(elems19[1],int(elems19[2]))[0]
        elems18[1] = genom18[0]
        elems18[2] = str(genom18[1])
        lines18.append('\t'.join(elems18))
    with open(input_path,'w') as f:
        f.write('\n'.join(lines18))
        




