'''
Usage:
>python get_flanking_seq.py chr1 185294
chr1:185294     GAGGGGCCCGGAGGAGCCTTTGCCCGC     TGTCAGACTCCATCCCTCCTCTGCCGCCA   -       ENST00000623834.3       335     1
chr1:185294     GAGGGGCCCGGAGGAGCCTTTGCCCGC     TGTCAGACTCCATCCCTCCTCTGCCGCCA   -       ENST00000623083.3       440     1
'''

import MySQLdb
import sys

def get_bef_aft (uid, apos, befseq, aftseq):
    for a in range(apos + 1, apos + 10):
        a = str(a)
        q = 'select bases from CodonTable where UID=' + uid + ' and Pos=' + a
        cursor.execute(q)
        r2 = cursor.fetchone()
        if r2:
            aftseq = aftseq + r2[0]
        else:
            aftseq = aftseq + '*'
    for a in range(apos - 1, apos - 10, -1):
        a = str(a)
        q = 'select bases from CodonTable where UID=' + uid + ' and Pos=' + a
        cursor.execute(q)
        r2 = cursor.fetchone()
        if r2:
            befseq = r2[0] + befseq
        else:
            befseq = '*' + befseq
    return befseq, aftseq

def get_transcript (uid):
    q = 'select EnsT from Transcript where UID=' + uid
    c2.execute(q)
    t = c2.fetchone()[0]
    return t

conn = MySQLdb.connect('karchin-db01', '', '', 'SNVBox_5')
cursor = conn.cursor()
c2 = conn.cursor()

chrom = sys.argv[1]
gpos = sys.argv[2]

befseq = ''
aftseq = ''

hit = False

q = 'select * from CodonTable where chrom="' + chrom + '" and pos1=' + gpos
cursor.execute(q)
rs = cursor.fetchall()
for r in rs:
    hit = True
    codonpos = 1
    (uid, apos, dummy, pos1, pos2, pos3, bases) = r
    uid = str(uid)
    if pos1 < pos3:
        strand = '+'
    else:
        strand = '-'
    befseq = ''
    aftseq = bases[1:]
    befseq, aftseq = get_bef_aft(uid, apos, befseq, aftseq)
    t = get_transcript(uid)
    print(chrom + ':' + gpos + '\t' + befseq + '\t' + aftseq + '\t' + strand + '\t' + t + '\t' + str(apos) + '\t' + str(codonpos))

q = 'select * from CodonTable where chrom="' + chrom + '" and pos2=' + gpos
cursor.execute(q)
rs = cursor.fetchall()
for r in rs:
    hit = True
    codonpos = 2
    (uid, apos, dummy, pos1, pos2, pos3, bases) = r
    uid = str(uid)
    if pos1 < pos3:
        strand = '+'
    else:
        strand = '-'
    befseq = bases[0]
    aftseq = bases[2]
    befseq, aftseq = get_bef_aft(uid, apos, befseq, aftseq)
    t = get_transcript(uid)
    print(chrom + ':' + gpos + '\t' + befseq + '\t' + aftseq + '\t' + strand + '\t' + t + '\t' + str(apos) + '\t' + str(codonpos))

q = 'select * from CodonTable where chrom="' + chrom + '" and pos3=' + gpos
cursor.execute(q)
rs = cursor.fetchall()
for r in rs:
    hit = True
    codonpos = 3
    (uid, apos, dummy, pos1, pos2, pos3, bases) = r
    uid = str(uid)
    if pos1 < pos3:
        strand = '+'
    else:
        strand = '-'
    befseq = bases[:2]
    aftseq = ''
    befseq, aftseq = get_bef_aft(uid, apos, befseq, aftseq)
    t = get_transcript(uid)
    print(chrom + ':' + gpos + '\t' + befseq + '\t' + aftseq + '\t' + strand + '\t' + t + '\t' + str(apos) + '\t' + str(codonpos))

if hit == False:
    print('no hit')
    exit()