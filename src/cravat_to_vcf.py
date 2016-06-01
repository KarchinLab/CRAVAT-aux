# Convert an entire test case (input, desc, key) from cravat to VCF.  
#Save the test case to a new dir with a new name
import os
import shutil

def switch_strand(seq, strand):
    pairs = {'A':'T','T':'A','G':'C','C':'G'}
    if seq == '-':
        out = '-'
    elif strand == '-':
        out = ''
        for nuc in seq:
            out += pairs[nuc]
    else:
        out = seq
    return out

test_cases = ['pop_stats']
source_dir = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\cravat'
dest_dir = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\vcf'

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
    c_dir = os.path.join(source_dir, test)
    v_dir = os.path.join(dest_dir, test)
    
    print '\tMaking folder and files'
    if os.path.exists(v_dir):
        cont = raw_input('\tVCF dir exists, continue? <y/n>: ')
        if cont == 'y':
            shutil.rmtree(v_dir)
            os.makedirs(v_dir)
        else:
            continue
    else:
        os.makedirs(v_dir)
    
    shutil.copyfile(os.path.join(c_dir,'%s_desc.xml' %test), os.path.join(v_dir,'%s_desc.xml' %test))
    shutil.copyfile(os.path.join(c_dir,'%s_key.csv' %test), os.path.join(v_dir,'%s_key.csv' %test))
    shutil.copyfile(os.path.join(c_dir,'%s_input.txt' %test), os.path.join(v_dir,'%s_input.txt' %test))
    
    print '\tConverting input to VCF'
    input_path = os.path.join(v_dir,'%s_input.txt' %test)
    c_text = open(input_path).read()
    c_lines = c_text.split('\n')
    v_lines = []
    for c_line in c_lines:
        c_vals = c_line.split('\t')
        v_vals =['','','','','','25','PASS','BLANK']
        v_vals[0] = c_vals[1]
        v_vals[1] = c_vals[2]
        v_vals[2] = c_vals[0]
        v_vals[3] = switch_strand(c_vals[4],c_vals[3])
        v_vals[4] = switch_strand(c_vals[5],c_vals[3])
        v_lines.append('\t'.join(v_vals))
    v_text = '##fileformat=VCFv4.1\n#CHROM    POS    ID    REF    ALT    QUAL    FILTER    INFO\n'
    v_text += '\n'.join(v_lines)
    
    with open(os.path.join(v_dir,'%s_input.txt' % test),'w') as v_file:
        v_file.write(v_text)
    print '\tCompleted'
print 'All completed'