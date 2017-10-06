dbtxtfilename = 'ncrna.txt'

def make_txt ():
    import MySQLdb

    conn = MySQLdb.connect('karchin-db01', 'rachelk', 'chrislowe', 'hg38')
    cursor = conn.cursor()

    repclasses_to_include = {
        'Low_complexity',
        'LINE',
        'SINE',
        'LTR',
        'Simple_repeat',
        'Satellite'
    }

    wf = open(dbtxtfilename, 'w')

    cursor.execute('select bin, genoName, genoStart, genoEnd, repName, ' +\
        'repFamily, repClass, strand from rmsk')
    for row in cursor.fetchall():
        (binno, chrom, start, end, repname, repfamily, repclass, strand) = row

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
            repfamily,
            repname
            ]) + '\n')


    wf.close()

def make_db ():
    import sqlite3

    conn = sqlite3.connect('repeat.sqlite')
    cursor = conn.cursor()
    tablename = 'repeat'
    cursor.execute('drop table if exists ' + tablename)
    cursor.execute('create table ' + tablename + ' (binno integer, ' +\
        'chrom text, start integer, end integer, strand text, ' +\
        'class text, family, name text)')
    cursor.execute('create index ' + tablename + '_idx on ' + tablename +\
        ' (binno, chrom, start)')
    f = open(dbtxtfilename)
    for line in f:
        [binno, chrom, start, end, strand, repclass, family, name] =\
            line[:-1].split('\t')
        sql = 'insert into ' + tablename + ' values(' +\
            '%s, "%s", %s, %s, "%s", "%s", "%s", "%s")'% \
            (binno, chrom, start, end, strand, repclass, family, name)
        cursor.execute(sql)
    f.close()
    conn.commit()

make_txt()
make_db()
