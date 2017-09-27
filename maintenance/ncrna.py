dbtxtfilename = 'ncrna.txt'

def make_txt ():
    import MySQLdb

    conn = MySQLdb.connect('karchin-db01', 'rachelk', 'chrislowe', 'hg38')
    cursor = conn.cursor()

    repclasses_to_include = {
        'rRNA',
        'scRNA',
        'snRNA',
        'srpRNA'
    }

    wf = open(dbtxtfilename, 'w')

    cursor.execute('select bin, genoName, genoStart, genoEnd, repName, ' +\
        'repClass, repFamily, strand from rmsk')
    for row in cursor.fetchall():
        (binno, chrom, start, end, repname, repclass, repfamily, strand) = row

        if repclass not in repclasses_to_include:
            continue

        binno = str(binno)
        start = str(start)
        end = str(end)
        wf.write('\t'.join([
            binno,
            chrom,
            start,
            end,
            strand,
            repclass,
            repfamily + ':' + repname
            ]) + '\n')

    cursor.execute('select bin, chrom, chromStart, chromEnd, strand, ' +\
        'aa from tRNAs')
    for row in cursor.fetchall():
        (binno, chrom, start, end, strand, aa) = row
        binno = str(binno)
        start = str(start)
        end = str(end)
        wf.write('\t'.join([
            binno,
            chrom,
            start,
            end,
            strand, 
            'tRNA',
            aa
            ]) + '\n')

    cursor.execute('select bin, chrom, chromStart, chromEnd, strand, ' +\
        'type, name from wgRna')
    for row in cursor.fetchall():
        (binno, chrom, start, end, strand, feattype, featname) = row
        binno = str(binno)
        start = str(start)
        end = str(end)
        wf.write('\t'.join([
            binno,
            chrom,
            start,
            end,
            strand,
            feattype,
            featname
            ]) + '\n')

    cursor.execute('select bin, chrom, exonStarts, exonEnds, strand, ' +\
        'name from lincRNAsTranscripts')
    for row in cursor.fetchall():
        (binno, chrom, exonstarts, exonends, strand, name) = row
        binno = str(binno)
        exonstarts = [v.strip() for v in exonstarts.strip().strip(',').split(',')]
        exonends = [v.strip() for v in exonends.strip().strip(',').split(',')]
        for i in xrange(len(exonstarts)):
            exonstart = exonstarts[i]
            exonend = exonends[i]
            wf.write('\t'.join([
                binno,
                chrom,
                exonstart,
                exonend,
                strand,
                'lincRNA',
                name + ':' + str(i + 1)
                ]) + '\n')

    wf.close()

def make_db ():
    import sqlite3

    conn = sqlite3.connect('ncrna.sqlite')
    cursor = conn.cursor()
    tablename = 'ncrna'
    cursor.execute('drop table if exists ' + tablename)
    cursor.execute('create table ' + tablename + ' (binno integer, ' +\
        'chrom text, start integer, end integer, strand text, ' +\
        'class text, name text)')
    cursor.execute('create index ' + tablename + '_idx on ' + tablename +\
        ' (binno, chrom, start)')
    f = open(dbtxtfilename)
    for line in f:
        [binno, chrom, start, end, strand, cls, name] = line[:-1].split('\t')
        sql = 'insert into ' + tablename + ' values(' +\
            '%s, "%s", %s, %s, "%s", "%s", "%s")'% \
            (binno, chrom, start, end, strand, cls, name)
        cursor.execute(sql)
    f.close()
    conn.commit()

make_db()
