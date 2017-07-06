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
            try:
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
                fout.write('\t'.join(toks_out)+'\n')
            except:
                print toks
                raise

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
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('path_in', help="Path to the input file")
    args_parser.add_argument('chain', help="Path to the chain file")
    args_parser.add_argument('-l', '--liftover', default='/bin/liftOver', help='Path to liftOver utility. Default is "/bin/liftOver"')
    args_parser.add_argument('-c', '--cleanup', action='store_true', help='Remove temp files on completion')
    args_parser.add_argument('-o', '--out', help='Specify an out path. Default is input path + ".38"')
    sys_args = args_parser.parse_args()
    
    liftover_util = os.path.abspath(sys_args.liftover)
    chain_path = sys_args.chain
    cravat19_path = os.path.abspath(sys_args.path_in)
    bed19_path = cravat19_path + '.19.bed'
    bed38_path = cravat19_path + '.38.bed'
    if sys_args.out:
        cravat38_path = os.path.abspath(sys_args.out)
    else:
        cravat38_path = cravat19_path+'.38'
    liftover_fail_path = cravat19_path + '.liftover.fail'
    print 'Making .bed file'
    cravat_to_bed(cravat19_path, bed19_path)
    liftover(sys_args.liftover, bed19_path, sys_args.chain, bed38_path, liftover_fail_path)
    fail_lines = count_lines(liftover_fail_path)
    if fail_lines:
        sys.stderr.write('WARNING liftOver Failures: %s\n' %liftover_fail_path)
    print 'Making output file'
    bed_to_cravat(bed38_path, cravat38_path)
    if sys_args.cleanup:
        os.remove(bed19_path)
        os.remove(bed38_path)
        if not(fail_lines):
            os.remove(liftover_fail_path)