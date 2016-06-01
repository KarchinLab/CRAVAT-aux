from pyliftover import LiftOver
import os
import xml.etree.ElementTree as ET
import shutil

test_cases = ['pop_stats']
source_dir = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\cravat'
dest_dir = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\hg18'

# Generate list of tests to run, either from dir names in test dir, or user input
if test_cases == ['all']:
    test_list = os.listdir(source_dir)
    for item in test_list[:]:
        # Ignore dirs that start with #
        if item.startswith('#'):
            test_list.remove(item)
else:
    test_list = test_cases
            
for test in test_list:
    print test
    hg19_dir = os.path.join(source_dir,test)
    hg18_dir = os.path.join(dest_dir,test)
    
    # Make new directory. Move files to new dir with updated names . All changes will be done here
    print '\tMaking folder and files'
    if os.path.exists(hg18_dir):
        cont = raw_input('\thg18 dir exists, continue? <y/n>: ')
        if cont == 'y':
            shutil.rmtree(hg18_dir)
            os.makedirs(hg18_dir)
        else:
            continue
    else:
        os.makedirs(hg18_dir)
        
    shutil.copy(os.path.join(hg19_dir,'%s_desc.xml' %test), os.path.join(hg18_dir,'%s_desc.xml' %test))
    shutil.copy(os.path.join(hg19_dir,'%s_input.txt' %test), os.path.join(hg18_dir,'%s_input.txt' %test)) 
    shutil.copy(os.path.join(hg19_dir,'%s_key.csv' %test), os.path.join(hg18_dir,'%s_key.csv' %test))    
    
    # Add a <hg18>on</hg18> tag to the desc.xml
    print '\tChanging desc file'
    desc_path = os.path.join(hg18_dir,'%s_desc.xml' %test)
    desc = ET.parse(desc_path)
    hg18 = ET.Element('hg18')
    hg18.text = 'on'
    desc.find('sub_params').append(hg18)
    desc.write(desc_path)
    
    # Shift genomic coordinates to hg18
    print '\tLifting over coordinates'
    input_path = os.path.join(hg18_dir,'%s_input.txt' %test)
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
    print '\tCompleted'
print 'All completed'