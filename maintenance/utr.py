import sqlite3
import MySQLdb
import sys

debug = False

db = MySQLdb.connect('karchin-db01', 'rachelk', 'chrislowe', 'hg38')
cursor = db.cursor()

cursor.execute('select distinct name from SNVBox_5.Transcript')
transcripts = [v[0] for v in cursor.fetchall()]
sys.stderr.write(str(len(transcripts)) + ' transcripts\n')

def write_updownstream (chrom, txstart, txend, transcript):
    interval = 2000
    upstream_start = txstart - interval
    upstream_end = txstart - 1
    binno = get_bin(upstream_start, upstream_end)
    wf.write('\t'.join([
        str(binno),
        chrom,
        str(upstream_start),
        str(upstream_end),
        transcript,
        '2k upstream']) + '\n')
    downstream_start = txend + 1
    downstream_end = txend + interval
    binno = get_bin(downstream_start, downstream_end)
    wf.write('\t'.join([
        str(binno),
        chrom,
        str(downstream_start),
        str(downstream_end),
        transcript,
        '2k downstream']) + '\n')
def write_utr (chrom, exonstart, exonend, cdsstart, cdsend, transcript):
    if exonstart >= cdsstart:
        utr5start = None
        utr5end = None
    elif exonstart < cdsstart:
        utr5start = exonstart
        if exonend < cdsstart:
            utr5end = exonend
        elif exonend == cdsstart:
            utr5end = exonend - 1
        elif exonend > cdsstart:
            utr5end = cdsstart - 1
    if exonend <= cdsend:
        utr3start = None
        utr3end = None
    elif exonend > cdsend:
        utr3end = exonend
        if exonstart > cdsend:
            utr3start = exonstart
        elif exonstart == cdsend:
            utr3start = exonstart + 1
        elif exonstart < cdsend:
            utr3start = cdsend + 1
    if utr5start != None and utr5end != None:
        wf.write('\t'.join([
            str(get_bin(utr5start, utr5end)), 
            chrom, 
            str(utr5start), 
            str(utr5end), 
            transcript, 
            '5\' UTR']) + '\n')
    if utr3start != None and utr3end != None:
        wf.write('\t'.join([
            str(get_bin(utr3start, utr3end)), 
            chrom, 
            str(utr3start), 
            str(utr3end), 
            transcript, 
            '3\' UTR']) + '\n')

def get_bin (start, end):
    firstshift = 17
    nextshift = 3
    binoffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]

    startbin = start
    endbin = end
    startbin >>= firstshift
    endbin >>= firstshift

    for binoffset in binoffsets:
        if startbin == endbin:
            return binoffset + startbin
        startbin >>= nextshift
        endbin >>= nextshift

    # No bin was found.
    return None

wf = open('utrint.txt', 'w')
count = 0
for transcript in transcripts:
    count += 1
    if count % 1000 == 0:
        sys.stderr.write(str(count) + '\n')
    sql = 'select chrom, txStart, txEnd, cdsStart, cdsEnd, exonStarts, ' +\
        'exonEnds, alignID from knownGene where name="' + transcript + '"'
    cursor.execute(sql)
    [chrom, txstart, txend, cdsstart, cdsend, exonstarts, exonends, enst]\
        = cursor.fetchone()
    transcript = enst
    # Makes "end"s inclusive.
    txend = txend - 1
    cdsend = cdsend - 1
    exonstarts = [int(v) for v in exonstarts.strip(',').split(',')]
    exonends = [int(v) - 1 for v in exonends.strip(',').split(',')]
    noexons = len(exonstarts)

    if debug:
        print 'txstart', txstart, 'cdsstart', cdsstart, 'txend', txend, \
            'cdsend', cdsend
        print 'exonstarts', exonstarts
        print 'exonends', exonends

    # Upstream and downstream
    write_updownstream(chrom, txstart, txend, transcript)

    # 1-exon transcript
    if len(exonstarts) == 1:
        exonstart = exonstarts[0]
        exonend = exonends[0]
        if cdsstart > exonstart:
            utr5start = exonstart
            utr5end = cdsstart - 1
            wf.write('\t'.join([
                str(get_bin(utr5start, utr5end)), 
                chrom, 
                str(utr5start), 
                str(utr5end), 
                transcript, 
                '5\' UTR']) + '\n')
        if cdsend < exonend:
            utr3start = cdsend + 1
            utr3end = exonend
            wf.write('\t'.join([
                str(get_bin(utr5start, utr5end)), 
                chrom, 
                str(utr3start), 
                str(utr3end), 
                transcript, 
                '3\' UTR']) + '\n')
        continue

    # Multiple-exon transcript
    for exonno in xrange(len(exonstarts) - 1):
        exonstart1 = exonstarts[exonno]
        exonend1 = exonends[exonno]
        exonstart2 = exonstarts[exonno + 1]

        # UTRs
        write_utr(chrom, exonstart1, exonend1, cdsstart, cdsend, transcript)

        # Intron
        intronstart = exonend1 + 1
        intronend = exonstart2 - 1
        wf.write('\t'.join([
            str(get_bin(intronstart, intronend)), 
            chrom, 
            str(exonend1 + 1), 
            str(exonstart2 - 1), 
            transcript, 
            'Intron']) + '\n')
    write_utr(chrom, exonstarts[-1], exonends[-1], cdsstart, cdsend, 
        transcript)
wf.close()

conn = sqlite3.connect('utrint.sqlite')
cursor = conn.cursor()
cursor.execute('drop table if exists utrint')
cursor.execute('create table utrint (bin integer, chrom text, start integer, end integer, enst text, desc text)')
cursor.execute('create index binchrstart on utrint (bin, chrom, start)')
