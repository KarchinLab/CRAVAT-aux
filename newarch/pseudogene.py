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
    exonends = [v.strip() for v in exonends.strip(',').split(',')]
    wf.write('\t'.join([str(tid), strand, enst, hugo]) + '\n')
    for i in xrange(len(exonstarts)):
        wfe.write('\t'.join([str(binno), chrom, exonstarts[i], exonends[i], 
            str(tid)]) + '\n')
wf.close()
wfe.close()
