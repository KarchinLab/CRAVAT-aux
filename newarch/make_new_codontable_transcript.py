# Run in hn05.
def make_file ():
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

def make_db ():
    import sqlite3
    conn = sqlite3.connect('knowngene.sqlite')
    cursor = conn.cursor()
    
    cursor.execute('drop table if exists map')
    cursor.execute('drop table if exists codon')
    cursor.execute('drop table if exists transcript')
    cursor.execute('drop table if exists transcript_info')
    
    cursor.execute('create table map (' +
        'binno integer, ' +
        'chrom text, ' +
        'gpos integer, ' +
        'tid integer, ' +
        'apos integer, ' +
        'posincodon integer, ' +
        'base text)')
    cursor.execute('create index map_idx1 on map ' +
        '(chrom, binno, gpos)')
    cursor.execute('create index map_idx2 on map ' +
        '(tid, apos)')
    
    cursor.execute('create table codon (' +
        'tid integer, ' +
        'apos integer, ' +
        'codon text)')
    cursor.execute('create index codon_idx1 on codon ' +
        '(tid)')
    
    cursor.execute('create table transcript (' +
        'tid integer, ' +
        'name text, ' +
        'source text)')
    cursor.execute('create index transcript_idx1 on transcript ' +
        '(tid)')
    
    cursor.execute('create table transcript_info (' +
        'tid integer, ' +
        'sprot text, ' +
        'hugo text, ' +
        'strand text, ' +
        'alen integer, ' +
        'chrom text)')
    cursor.execute('create index transcript_info_idb1 on transcript_info '
        '(tid)')