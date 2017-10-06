def make_file ():
    import MySQLdb
    conn = MySQLdb.connect('karchin-db01', 'rachelk', 'chrislowe', 'hg38')
    cursor = conn.cursor()
    cursor.execute('select bin, name ,chrom, strand, exonStarts, exonEnds, ' +
        'name2 from wgEncodeGencodePseudoGeneV24')
    wf = open('pseudogene.transcript.txt', 'w')
    wfe = open('pseudogene.exon.txt', 'w')
    tid = 0
    for row in cursor.fetchall():
        tid += 1
        (binno, enst, chrom, strand, exonstarts, exonends, hugo) = row
        exonstarts = [v.strip() for v in exonstarts.strip(',').split(',')]
        exonends = [str(int(v.strip()) - 1) for v in 
            exonends.strip(',').split(',')]
        wf.write('\t'.join([str(tid), strand, enst, hugo]) + '\n')
        for i in xrange(len(exonstarts)):
            wfe.write('\t'.join([str(binno), chrom, exonstarts[i], exonends[i], 
                str(tid)]) + '\n')
    wf.close()
    wfe.close()

def make_db ():
    import sqlite3
    conn = sqlite3.connect('pseudogene.sqlite')
    cursor = conn.cursor()
    cursor.execute('drop table if exists transcript')
    cursor.execute('drop table if exists exon')
    cursor.execute('create table exon (' +
        'binno integer, chrom text, start integer, end integer, tid integer)')
    cursor.execute('create index exon_idx1 on exon ' +
        '(chrom, binno)')
    cursor.execute('create table transcript (' +
        'tid integer, strand text, enst text, hugo text)')
    cursor.execute('create index transcript_idx1 on transcript ' +
        '(tid)')

make_db()
