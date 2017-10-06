'''
Run inside a cravatmupit container.
'''

import MySQLdb

conn = MySQLdb.connect('karchin-db01', 'rachelk', 'chrislowe', 'SNVBox_5')
cursor = conn.cursor()

cursor.execute('select distinct UID from CodonTable')
tids = [v[0] for v in cursor.fetchall()]
tids.sort()

def get_bin (pos):
    pos = pos >> 17
    return str(pos)

wf = open('g2c.txt', 'w')
wf_codon = open('codons.txt', 'w')
wf_tr = open('transcripts.txt', 'w')
wf_trinfo = open('transcript_info.txt', 'w')
count = 0
print len(tids), 'transcripts to process'
for tid in tids:
    print tid
    count += 1 
    if count % 30 == 0:
        pass
    print '  querying codontable'
    cursor.execute('select * from CodonTable where UID=' + str(tid))
    print '  done'
    codontablerows = cursor.fetchall()
    [tid, apos, chrom, pos1, pos2, pos3, codon] = codontablerows[0]
    if pos1 < pos3:
        strand = '+'
    else:
        strand = '-'
    print '  querying transcript'
    cursor.execute('select * from Transcript where UID=' + str(tid))
    print '  done'
    [tid, kgname, hugo, chrom, ccds, nm, np, enst, ensp, sprot, aalen] =\
        cursor.fetchone()
    tid = str(tid)
    if kgname is not None:
        wf_tr.write('\t'.join([
            tid,
            kgname,
            'KnownGene']) + '\n')
    if ccds is not None:
        wf_tr.write('\t'.join([
            tid,
            ccds,
            'CCDS']) + '\n')
    if nm is not None:
        wf_tr.write('\t'.join([
            tid,
            nm,
            'RefSeqT']) + '\n')
    if np is not None:
        wf_tr.write('\t'.join([
            tid,
            np,
            'RefSeqP']) + '\n')
    if enst is not None:
        wf_tr.write('\t'.join([
            tid,
            enst,
            'EnsemblT']) + '\n')
    if ensp is not None:
        wf_tr.write('\t'.join([
            tid,
            ensp,
            'EnsemblP']) + '\n')
    if sprot == None:
        sprot = ''
    if hugo == None:
        hugo = ''
    wf_trinfo.write('\t'.join([
        tid,
        sprot,
        hugo,
        strand,
        str(aalen),
        chrom]) + '\n')
    aposs = [v[1] for v in codontablerows]
    aposs.sort()
    codontabledic = {}
    for row in codontablerows:
        codontabledic[row[1]] = row
    print '  handling aposs'
    for apos in aposs:
        [tid, apos, chrom, pos1, pos2, pos3, codon] = codontabledic[apos]
        tid = str(tid)
        apos = str(apos)
        pos1 = pos1
        pos2 = pos2
        pos3 = pos3
        pos1bin = get_bin(pos1)
        pos2bin = get_bin(pos2)
        pos3bin = get_bin(pos3)
        wf.write('\t'.join([
            pos1bin,
            chrom,
            str(pos1),
            tid,
            apos,
            '0',
            codon[0]]) + '\n')
        wf.write('\t'.join([
            pos2bin,
            chrom,
            str(pos2),
            tid,
            apos,
            '1',
            codon[1]]) + '\n')
        wf.write('\t'.join([
            pos3bin,
            chrom,
            str(pos3),
            tid,
            apos,
            '2',
            codon[2]]) + '\n')
        wf_codon.write('\t'.join([
            tid,
            apos,
            codon]) +'\n')
    print '  done'
wf.close()
wf_tr.close()
wf_trinfo.close()
wf_codon.close()
