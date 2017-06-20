# Convert an entire test case (input, desc, key) from cravat to VCF. 
# Save the test case to a new dir with a new name

import os
import shutil
import sys

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

if __name__ == '__main__':
    basedir = sys.argv[1]
    cases = sys.argv[2].split(',')
    for case in cases:
        print 'Case:', case
        c_test = case+'_c'
        top_dir = os.path.join(basedir, case)
        v_test = '_'.join(c_test.split('_')[:-1])+'_v'
        
        c_dir = os.path.join(top_dir, c_test)
        v_dir = os.path.join(top_dir, v_test)
        print 'src:', c_dir
        print 'dst:', v_dir
        
        print '\tMaking folder and files'
        if os.path.exists(v_dir):
            shutil.rmtree(v_dir)
        os.makedirs(v_dir)
        
        shutil.copyfile(os.path.join(c_dir,'%s_desc.xml' %c_test), os.path.join(v_dir,'%s_desc.xml' %v_test))
        shutil.copyfile(os.path.join(c_dir,'%s_key.csv' %c_test), os.path.join(v_dir,'%s_key.csv' %v_test))
        shutil.copyfile(os.path.join(c_dir,'%s_input.txt' %c_test), os.path.join(v_dir,'%s_input.txt' %v_test))
        
        print '\tConverting input to VCF'
        input_path = os.path.join(v_dir,'%s_input.txt' %v_test)
        c_text = open(input_path).read()
        c_lines = c_text.split('\n')
        v_lines = []
        for c_line in c_lines:
            if not(c_line):
                continue
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
        
        with open(os.path.join(v_dir,'%s_input.txt' %v_test),'w') as v_file:
            v_file.write(v_text)
        print '\tCompleted'
    print 'All completed'