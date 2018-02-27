# Run it where hg38 is available (hn05).

import sqlite3
import MySQLdb
import sys

debug = False

db = MySQLdb.connect('karchin-db01', 'rachelk', 'chrislowe', 'hg38')
cursor = db.cursor()

cursor.execute('select distinct name from SNVBox_5.Transcript')
transcripts = [v[0] for v in cursor.fetchall()]
sys.stderr.write(str(len(transcripts)) + ' transcripts\n')

def write_updownstream (chrom, txstart, txend, transcript, hugo, strand):
    interval = 2000
    upstream_start = txstart - interval
    upstream_end = txstart - 1
    binno = get_bin(upstream_start, upstream_end)
    if strand == '+':
        upstr = '2k upstream'
        dnstr = '2k downstream'
    elif strand == '-':
        upstr = '2k downstream'
        dnstr = '2k upstream'
    wf.write('\t'.join([
        chrom,
        str(binno),
        str(upstream_start),
        str(upstream_end),
        transcript,
        hugo,
        upstr]) + '\n')
    downstream_start = txend + 1
    downstream_end = txend + interval
    binno = get_bin(downstream_start, downstream_end)
    wf.write('\t'.join([
        chrom,
        str(binno),
        str(downstream_start),
        str(downstream_end),
        transcript,
        hugo,
        dnstr]) + '\n')
    
def write_utr (chrom, exonstart, exonend, cdsstart, cdsend, transcript, strand):
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
        if strand == '+':
            utrstr = '5\' UTR'
        elif strand == '-':
            utrstr = '3\' UTR'
        wf.write('\t'.join([
            chrom, 
            str(get_bin(utr5start, utr5end)), 
            str(utr5start), 
            str(utr5end), 
            transcript, 
            hugo,
            utrstr]) + '\n')
    if utr3start != None and utr3end != None:
        if strand == '+':
            utrstr = '3\' UTR'
        elif strand == '-':
            utrstr = '5\' UTR'
        wf.write('\t'.join([
            chrom, 
            str(get_bin(utr3start, utr3end)), 
            str(utr3start), 
            str(utr3end), 
            transcript, 
            hugo,
            utrstr]) + '\n')

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
        'exonEnds, alignID, strand from knownGene where name="' + transcript + '"'
    cursor.execute(sql)
    [chrom, txstart, txend, cdsstart, cdsend, exonstarts, exonends, enst, \
        strand] = cursor.fetchone()
    sql = 'select geneSymbol from kgXref where kgID="' + transcript + '"'
    cursor.execute(sql)
    hugo = cursor.fetchone()
    if hugo == None:
        print(transcript, hugo)
        exit()
    else:
        hugo = hugo[0]

    transcript = enst

    # starts in hg38 are 0-based.
    txstart += 1
    cdsstart += 1
    exonstarts = [int(v) + 1 for v in exonstarts.strip(',').split(',')]
    
    txend = txend
    cdsend = cdsend
    exonends = [int(v) for v in exonends.strip(',').split(',')]
    
    noexons = len(exonstarts)

    if debug:
        print('txstart', txstart, 'cdsstart', cdsstart, 'txend', txend, \
            'cdsend', cdsend)
        print('exonstarts', exonstarts)
        print('exonends', exonends)

    # Upstream and downstream
    write_updownstream(chrom, txstart, txend, transcript, hugo, strand)

    # 1-exon transcript
    if len(exonstarts) == 1:
        exonstart = exonstarts[0]
        exonend = exonends[0]
        if cdsstart > exonstart:
            utr5start = exonstart
            utr5end = cdsstart - 1
            if strand == '+':
                utrstr = '5\' UTR'
            elif strand == '-':
                utrstr = '3\' UTR'
            wf.write('\t'.join([
                chrom, 
                str(get_bin(utr5start, utr5end)), 
                str(utr5start), 
                str(utr5end), 
                transcript, 
                hugo,
                utrstr]) + '\n')
        if cdsend < exonend:
            utr3start = cdsend + 1
            utr3end = exonend
            if strand == '+':
                utrstr = '3\' UTR'
            elif strand == '-':
                utrstr = '5\' UTR'
            wf.write('\t'.join([
                chrom, 
                str(get_bin(utr5start, utr5end)), 
                str(utr3start), 
                str(utr3end), 
                transcript, 
                hugo,
                utrstr]) + '\n')
        continue

    # Multiple-exon transcript
    for exonno in range(len(exonstarts) - 1):
        exonstart1 = exonstarts[exonno]
        exonend1 = exonends[exonno]
        exonstart2 = exonstarts[exonno + 1]

        # UTRs
        write_utr(
            chrom, 
            exonstart1, 
            exonend1, 
            cdsstart, 
            cdsend, 
            transcript,
            strand)

        # Intron
        intronstart = exonend1 + 1
        intronend = exonstart2 - 1
        wf.write('\t'.join([
            chrom, 
            str(get_bin(intronstart, intronend)), 
            str(exonend1 + 1), 
            str(exonstart2 - 1), 
            transcript,
            hugo,
            'intron']) + '\n')
    write_utr(
        chrom, 
        exonstarts[-1], 
        exonends[-1], 
        cdsstart, 
        cdsend, 
        transcript,
        strand)
wf.close()

conn = sqlite3.connect('utrint.sqlite')
cursor = conn.cursor()
cursor.execute('drop table if exists utrint')
cursor.execute('create table utrint (chrom text, binno integer, start integer, end integer, enst text, hugo text, desc text)')
cursor.execute('create index binchrstart on utrint (chrom, binno, start)')
