import os
import subprocess
import sys
import xml.etree.ElementTree as ET

modbase_dir = 'C:\\Projects\\CRAVAT\\Modbase\\ModBase_H_sapiens_2013_refseq'
alignment_dir = os.path.join(modbase_dir, 'alignments\\alignment')
model_dir = os.path.join(modbase_dir, 'models\\model')

def filter_by_seq_iden():
    
    seq_iden_cutoff = 95.0
    
    f = open('C:\\Projects\\CRAVAT\\Modbase\\ModBase_H_sapiens_2013_refseq\\H_sapiens_2013_refseq.summary.txt')
    f.readline()
    for line in f:
        [dummy, target, target_beg, target_end, seq_iden, evalue, dummy, dummy, dummy, pdb_code, pdb_chain, pdb_beg, pdb_end, dummy, dummy, dummy, dummy] = line[:-1].split('\t')
        seq_iden = float(seq_iden)
        if seq_iden >= seq_iden_cutoff:
            os.path.join(alignment_dir, target)
        sys.exit()

def decompress_align_xz ():
    files = os.listdir(alignment_dir)
    count = 0
    for file in files:
        if file.endswith('.xz'):
            output = subprocess.check_output(['\\bin\\xz\\bin_x86-64\\xzdec', os.path.join(alignment_dir, file)])
            wf = open(os.path.join(alignment_dir, file[:-3]), 'w')
            wf.write(output)
            wf.close()
            count += 1
            if count % 1000 == 0:
                print count, file

def decompress_model_xz ():
    files = os.listdir(model_dir)
    count = 0
    for file in files:
        if file.endswith('.xz'):
            output = subprocess.check_output(['\\bin\\xz\\bin_x86-64\\xzdec', os.path.join(model_dir, file)])
            wf = open(os.path.join(model_dir, file[:-3]), 'w')
            wf.write(output)
            wf.close()
            count += 1
            if count % 100 == 0:
                print count, file

decompress_model_xz()