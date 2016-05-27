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

test_name = 'transcript'
main_dir = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases'

c_dir = os.path.join(main_dir,test_name)
v_dir = os.path.join(main_dir,'%s_vcf' %test_name)

print 'CRAVAT dir: %s' %c_dir

with open(os.path.join(c_dir,'%s_input.txt' %test_name)) as c_file:
    c_text = c_file.read()
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

if not(os.path.exists(v_dir)):
    os.makedirs(v_dir)
with open(os.path.join(v_dir,'%s_vcf_input.txt' % test_name),'w') as v_file:
    v_file.write(v_text)

shutil.copyfile(os.path.join(c_dir,'%s_desc.xml' %test_name), os.path.join(v_dir,'%s_vcf_desc.xml' %test_name))
shutil.copyfile(os.path.join(c_dir,'%s_key.csv' %test_name), os.path.join(v_dir,'%s_vcf_key.csv' %test_name))
print 'VCF dir: %s' %v_dir