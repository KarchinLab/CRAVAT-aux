"""

This program prints to STDOUT the protein length of each transcript 
provided by the input file. The input file's format is one of the following 
for each line:

UID<space>CCDS<space>NM<space>NP<space>ENST<space>ENSP

The output's format is, for each line, one of the following:

UID<space>HUGO Symbol<space>CCDS<space>NM<space>NP<space>ENST<space>ENSP<space>
"""

import MySQLdb
import os
import subprocess
import sys

def get_all_transcript_list ():
    db = MySQLdb.connect(user='andywong86',passwd='andy_wong_mysql+86',db='CHASM',host='127.0.0.1')
    cursor = db.cursor()
    cursor.execute('select CCDS, RefseqP, EnsP from Transcript')
    wf = open('accs.txt', 'w')
    for ccds, refseqp, ensp in cursor.fetchall():
        acc = None
        if refseqp != None:
            acc = refseqp
        elif ccds != None:
            acc = ccds
        elif ensp != None:
            acc = ensp
        else:
            print 'Acc error:', refseqp, ',', ccds, ',', ensp
        wf.write(acc+'\n')
    wf.close()

fa_base_dir = '/home/pipeline/snv1304/build/t2k-hmms/'
refseq_dir = fa_base_dir + '/' + 'refseq'
ccds_dir = fa_base_dir + '/' + 'CCDS'
ensembl_dir = fa_base_dir + '/' + 'Ensembl'

uid_symbol_dic = {}
f = open('/home/pipeline/CHASM/uid_symbol.txt')
for line in f:
    toks = line.strip().split('\t')
    symbol = toks[0]
    uid = toks[1]
    uid_symbol_dic[uid] = symbol
f.close()

f = open(sys.argv[1])
for line in f:
    toks = line.strip().replace(' ', '\t').split('\t')
    uid = None
    ccds = None
    nm = None
    np = None
    enst = None
    ensp = None
    if len(toks) == 2: # NM, NP or ENST, ENSP
        p_head = toks[1][:2]
        if p_head == 'NP':
            nm = toks[0]
            np = toks[1]
        elif p_head == 'EN':
            enst = toks[0]
            ensp = toks[1]
    elif len(toks) == 1: # CCDS
        ccds = toks[0]
    elif len(toks) == 6: # UID, CCDS, NM, NP, ENST, ENSP
        uid = toks[0]
        ccds = toks[1]
        nm = toks[2]
        np = toks[3]
        enst = toks[4]
        ensp = toks[5]
    if uid == '\N': uid = None
    if ccds == '\N': ccds = None
    if nm == '\N': nm = None
    if np == '\N': np = None
    if enst == '\N': enst = None
    if ensp == '\N': ensp = None
    if np != None:
        fa_filename = refseq_dir + '/' + np[3:7] + '/' + np.split('.')[0] + '/' + np + '.fa'
        acc_type = 'refseq'
    elif ccds != None:
        fa_filename = ccds_dir + '/' + ccds[4] + '/' + ccds.split('.')[0] + '/' + ccds + '.fa'
        acc_type = 'ccds'
    elif ensp != None:
        fa_filename = ensembl_dir + '/' + ensp[-4:] + '/' + ensp + '/' + ensp + '.fa'
        acc_type = 'ensembl'
    else:
        print 'error',toks
        sys.exit()
    symbol = uid_symbol_dic[uid]
    aa_len = 0
    rerun = False
    if os.path.exists(fa_filename) == True:
        f2 = open(fa_filename)
        f2.readline()
        line2 = f2.readline().strip()
        aa_len = 0
        while len(line2) > 0:
            aa_len += len(line2)
            line2 = f2.readline().strip()
        f2.close()
        if aa_len == 0:
            rerun = True
    if os.path.exists(fa_filename) == False or rerun == True:
        if acc_type == 'refseq':
            wf = open('tmp_acc.txt', 'w')
            wf.write(nm.split('.')[0]+'\n')
            wf.close()
            ret = subprocess.call('/programs/kent/bin/x86_64/refSeqGet hg19 -accList=/home/pipeline/CHASM/tmp_acc.txt -protSeqs='+fa_filename, shell=True)
        elif acc_type == 'ccds':
            ret = subprocess.call('/programs/kent/bin/x86_64/hgsql -Ne \"select * from ccdsGene where name=\\\"' + ccds + '\\\" limit 1\" hg19 | cut -f 2-16 | /programs/kent/bin/x86_64/getRnaPred -peptides hg19 stdin all ' + fa_filename, shell=True)
        elif acc_type == 'ensembl':
            ret = subprocess.call('/programs/kent/bin/x86_64/hgsql -Ne \"select * from ensPep where name=\\\"' + enst + '\\\"\" hg19 | awk \'{print \">\"$1; print $2}\' > ' + fa_filename, shell=True)
        f2 = open(fa_filename)
        f2.readline()
        line2 = f2.readline().strip()
        aa_len = 0
        while len(line2) > 0:
            aa_len += len(line2)
            line2 = f2.readline().strip()
        f2.close()
    print str(uid) + '\t' + \
          symbol + '\t' + \
          str(ccds) + '\t' + \
          str(nm) + '\t' + \
          str(np) + '\t' + \
          str(enst) + '\t' + \
          str(ensp) + '\t' + \
          str(aa_len)
f.close()