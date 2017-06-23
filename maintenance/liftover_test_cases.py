import subprocess
import os
import sys
import shutil
import argparse

''' This program will use USCS liftOver to change test cases from hg19 to hg18 or hg38.
    It must be run within a linux environment to properly use the liftOver utility.
    
    CRAVAT Format:
    0   1   2   3      4   5   6
    uid chr pos strand ref alt sample
        
        sample column is optional
    
    BED Format:
    0   1     2   3    4     5
    chr start end name score strand
    
        fill blank cols with .
'''
def count_lines(path,has_header=False):
    with open(path) as f:
        n = 0
        while f.readline():
            n += 1
    if has_header:
        n -= 1
    return n

def format_chromosome(chrom):
    if not(chrom.startswith('chr')):
        chrom = 'chr' + chrom
    if chrom[-1] in ['x','y']:
        chrom = chr[:-1] + chr[-1].upper()
    return chrom
    
def cravat_to_bed(cravat_path, bed_path):
    with open(cravat_path,'rU') as f, open(bed_path,'w') as fout:
        for l in f:
            l = l.replace('\n','')
            toks = l.split('\t')
#             print 'CRV:',toks
            if len(toks) < 7:
                toks.append(None)
            uid, chrom, start, strand, ref, alt, sample = toks
            chrom = format_chromosome(chrom)
            bed_id = '`'.join([uid, ref, alt])
            if sample:
                bed_id = '`'.join([bed_id, sample])
            if strand == '+':
                end = str(int(start) + len(ref))
            else:
                end = str(int(start) + 1)
                start = str(int(start) - len(ref) + 1)
            toks_out = [chrom, start, end, bed_id, '.', strand]
#             print 'BED:',toks_out
            fout.write('\t'.join(toks_out)+'\n')

def liftover(liftover_path, bed_path, chain_path, out_path, fail_path):
    args = [liftover_path, bed_path, chain_path, out_path, fail_path]
    # print ' '.join(args)
    subprocess.call(args)
    
def bed_to_cravat(bed_path, cravat_path):
    with open(bed_path,'rU') as f, open(cravat_path,'w') as fout:
        for l in f:
            l = l.replace('\n','')
            toks = l.split('\t')
#             print 'BED:',toks
            chrom, start, end, bed_id, _, strand = toks
            if strand == '+':
                cravat_pos = start
            else:
                cravat_pos = str(int(end) - 1)
            bed_id_info = bed_id.split('`')
            has_strand = len(bed_id_info) == 4
            toks_out = [None, chrom, cravat_pos, strand, None, None]
            if has_strand:
                toks_out.append(None)
                toks_out[0], toks_out[4], toks_out[5], toks_out[6] = bed_id_info
            else:
                toks_out[0], toks_out[4], toks_out[5] = bed_id_info
#             print 'CRV:',toks_out
            fout.write('\t'.join(toks_out)+'\n')
                
            
if __name__ == '__main__':
    test_cases_dir = sys.argv[1]
    command = sys.argv[2]
    do_liftover = False
    do_cleanup = False
    if command == 'lift':
        do_liftover = True
    elif command == 'clean':
        do_cleanup = True
    elif command == 'both':
        do_liftover = True
        do_cleanup = True
    else:
        print 'Command "%s" not recognized' %command
        exit()
    chain_path = '/home/kyle/liftOverChains/hg19ToHg38.over.chain'
    cases = os.listdir(test_cases_dir)
    for case in cases:
        case_top_dir = os.path.join(test_cases_dir, case)
        if os.path.isdir(case_top_dir):
            print '\n','='*10,case,'='*10
            if case in ['error','parsing','transcript','web_service']:
                print 'Case must be manually converted. Skipping'
                continue
            hg19_dir = os.path.join(case_top_dir, case+'_c')
            hg38_dir = os.path.join(case_top_dir, case+'_38')
            if do_liftover:
                if os.path.exists(hg38_dir):
                    shutil.rmtree(hg38_dir)
                shutil.copytree(hg19_dir, hg38_dir)
                hg38_files = os.listdir(hg38_dir)
                for cravat19 in hg38_files:
                    if cravat19.endswith('.txt'):
                        cravat19_path = os.path.join(hg38_dir, cravat19)
                        bed19_path = cravat19_path + '.19.bed'
                        bed38_path = cravat19_path + '.38.bed'
                        cravat38_path = cravat19_path+'.38'
                        liftover_fail_path = cravat19_path + '.liftover.fail'
                        cravat_to_bed(cravat19_path, bed19_path)
                        liftover('liftOver', bed19_path, chain_path, bed38_path, liftover_fail_path)
                        fail_lines = count_lines(liftover_fail_path)
                        if fail_lines:
                            print 'WARNING liftOver Failures: ',liftover_fail_path
                        # print 'BED38 to CRAVAT38'
                        # print 'cravat38:',cravat38_path
                        bed_to_cravat(bed38_path, cravat38_path)
            if do_cleanup:
                print hg19_dir
                print hg38_dir
                hg19_files = os.listdir(hg19_dir)
                for old_name in hg19_files:
                    new_name = old_name.replace('_c_','_19_')
                    old_path = os.path.join(hg19_dir, old_name)
                    new_path = os.path.join(hg19_dir, new_name)
                    shutil.move(old_path, new_path)
                hg38_files = os.listdir(hg38_dir)
                for fname in hg38_files:
                    fpath = os.path.join(hg38_dir, fname)
                    if fname.endswith('.liftover.fail'):
                        os.remove(fpath)
                    elif fname.endswith('.bed'):
                        os.remove(fpath)
                    elif fname.endswith('.38'):
                        new_fname = fname.replace('.38','')
                        new_fpath = os.path.join(hg38_dir, new_fname)
                        shutil.move(fpath, new_fpath)
                hg19_dirname = os.path.basename(hg19_dir)
                new_dirname = hg19_dirname.replace('_c','_19')
                shutil.move(hg19_dir, os.path.join(case_top_dir, new_dirname))
                hg38_dirname = os.path.basename(hg38_dir)
                new_dirname = hg19_dirname.replace('_38','_c')
                shutil.move(hg38_dir, os.path.join(case_top_dir, new_dirname))