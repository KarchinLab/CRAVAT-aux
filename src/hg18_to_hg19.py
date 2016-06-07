from pyliftover import LiftOver
import os
import xml.etree.ElementTree as ET
import shutil

hg19_test = 'chasm_gene_c'
top_dir = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\chasm_gene'
lo = LiftOver('hg19','hg18')
hg18_test = '_'.join(hg19_test.split('_')[:-1]) + '_18'

hg19_dir = os.path.join(top_dir, hg19_test)
hg18_dir = os.path.join(top_dir, hg18_test)

# Make new directory. Move files to new dir with updated names . All changes will be done here
print 'Making folder and files'
if os.path.exists(hg18_dir):
    cont = raw_input('hg18 dir exists, continue? <y/n>: ')
    if cont == 'y':
        shutil.rmtree(hg18_dir)
        os.makedirs(hg18_dir)
    else:
        exit()
else:
    os.makedirs(hg18_dir)
    
shutil.copy(os.path.join(hg19_dir,'%s_desc.xml' %hg19_test), os.path.join(hg18_dir,'%s_desc.xml' %hg18_test))
shutil.copy(os.path.join(hg19_dir,'%s_input.txt' %hg19_test), os.path.join(hg18_dir,'%s_input.txt' %hg18_test)) 
shutil.copy(os.path.join(hg19_dir,'%s_key.csv' %hg19_test), os.path.join(hg18_dir,'%s_key.csv' %hg18_test))    

# Add a <hg18>on</hg18> tag to the desc.xml
print 'Changing desc file'
desc_path = os.path.join(hg18_dir,'%s_desc.xml' %hg18_test)
desc = ET.parse(desc_path)
hg18 = ET.Element('hg18')
hg18.text = 'on'
desc.find('sub_params').append(hg18)
desc.write(desc_path)

# Shift genomic coordinates to hg18
print 'Lifting over coordinates'
input_path = os.path.join(hg18_dir,'%s_input.txt' %hg18_test)
input_text = open(input_path,'r').read()
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
print 'Completed'
print 'All completed'