'''
First task:
    UCSC -> download bigWigToBedGraph -> put in web02
    UCSC -> download Alignability 75mer track -> put in web02
    (on web02) ./bigWigToBedGraph align75.bigWig align75.bed
    align75.bed -> laptop

(not necessary) Second task:
    on hn04, in mysql, using SNVBox_dev,
        select Chr, cStart, cEnd from Transcript_Exon into outfile '/home/rkim/mysqlout/Transcript_Exon_Chr_cStart_cEnd.txt';
    Copy the txt file to laptop's c:\\datasources\\snvbox\\
        
Third task:
    Create CRAVAT_ANNOTATION database in karchin-db01 MySQL.
    GRANT SELECT ON `CRAVAT_ANNOTATION`.* TO 'cravat_user'@'karchin-web02.icm.jhu.edu';
'''
import MySQLdb
import os
import sys

chrLens = {'chr1':249250621,
           'chr2':243199373,
           'chr3':198022430,
           'chr4':191154276,
           'chr5':180915260,
           'chr6':171115067,
           'chr7':159138663,
           'chr8':146364022,
           'chr9':141213431,
           'chr10':135534747,
           'chr11':135006516,
           'chr12':133851895,
           'chr13':115169878,
           'chr14':107349540,
           'chr15':102531392,
           'chr16':90354753,
           'chr17':81195210,
           'chr18':78077248,
           'chr19':59128983,
           'chr20':63025520,
           'chr21':48129895,
           'chr22':51304566,
           'chrX':155270560,
           'chrY':59373566};

blacklist_cats = {
    'Low_artifact_island':'LMI', 
    'High_artifact_island':'HMI', 
    'snRNA':'snR', 
    '(GAATG)n':'GAA', 
    'centromeric_repeat':'CNR', 
    '(GAGTG)n':'GAG', 
    'SSU-rRNA_Hsa':'SSU', 
    'ALR/Alpha':'ALR', 
    'BSR/Beta':'BSR', 
    'Satellite_repeat':'STL', 
    'telomeric_repeat':'TLM', 
    'TAR1':'TAR', 
    'LSU-rRNA_Hsa':'LSU', 
    'chrM':'CHM', 
    '(CATTC)n':'CAT', 
    'ACRO1':'ACRO1',
    'HSATII':'TII',
    'Alignability_75mer':'A75'}

mysql_host = 'karchin-db01.icm.jhu.edu'
mysql_user = 'mryan'
mysql_password = 'royalenfield'
db_name = 'CRAVAT_ANNOTATION'

mapdir = 'c:\\projects\\cravat\\artifact'
# mapdir = '.'
exondir = 'c:\\datasources\\snvbox'

class Bin:
    offsetBasic = [512 + 64 + 8 + 1, 64 + 8 + 1, 8 + 1, 1, 0]
    offsetExtended = [4096 + 512 + 64 + 8 + 1, 512 + 64 + 8 + 1, 64 + 8 + 1, 8 + 1, 1, 0]
    firstShift = 17
    nextShift = 3
    maxEndBasic = 512 * 1024 * 1024
    extendFirstBin = 4681
    
    @staticmethod
    def getBin(start, stop, getall=False):
        # Assumes that start and end are all 1-based.
        if stop > Bin.maxEndBasic:
            offsets = Bin.offsetExtended
            firstBin = Bin.extendFirstBin
        else:
            offsets = Bin.offsetBasic
            firstBin = 0
        startBin = start >> Bin.firstShift
        endBin = stop >> Bin.firstShift
        
        bins = []
        
        for offset in offsets:
            if startBin == endBin:
                bin = offset + firstBin + startBin
                if getall:
                    bins.append(bin)
                else:
                    return bin
            else:
                startBin = startBin >> Bin.nextShift
                endBin = endBin >> Bin.nextShift
        if getall:
            return bins
        else:
            raise Exception("Cannot compute bin for %d and %d" % (start, stop))
    
def findAndWriteBadRegions (bedfilepath, cutoff):
#     bedfilepath = sys.argv[1]
    
    f = open(bedfilepath+'.bed')
    wf = open(bedfilepath + '.' + str(cutoff) + '.bed', 'w')
    
    blockchr = ''
    blockstart = -1
    blockstop = -1
    for line in f:
        [chr, start, stop, s] = line.rstrip().split('\t')
        s = float(s)
        # Purges the bad region at the change of chromosome.
        if blockchr != '' and blockchr != chr:
            wf.write(blockchr + '\t' + blockstart + '\t' + blockstop + '\t' + str(int(blockstop) - int(blockstart)) + '\n')
            blockchr = ''
            blockstart = -1
            blockstop = -1
        # Encounters a bad region.
        if s <= cutoff:
            # New encounter
            if blockstart == -1:
                blockchr = chr
                blockstart = start
                blockstop=  stop
            # Extends the existing block.
            else:
                blockstop = stop
        # Encounters a good region.
        else:
            # No block has started yet.
            if blockstart == -1:
                continue
            # A block has just ended before.
            else:
                wf.write(blockchr + '\t' + blockstart + '\t' + blockstop + '\t' + str(int(blockstop) - int(blockstart)) + '\n')
                blockchr = ''
                blockstart = -1
                blockstop = -1
    f.close()
    wf.close()

def getBadRegionPercent (bedfilepath):
#     bedfilepath = sys.argv[1]
    
    genomeSize = sum(chrLens.values())
    
    fragSizeSum = 0
    biggestFragSize = 0
    
    f = open(bedfilepath)
    for line in f:
        [chr, start, stop, dummy] = line.rstrip().split('\t')
        fragSize = int(stop) - int(start)
        if fragSize > biggestFragSize:
            biggestFragSize = fragSize
        fragSizeSum += fragSize
    f.close()
    
    print 'Genome size\t' + str(genomeSize)
    print 'Biggest bad alignability block size\t' + str(biggestFragSize)
    print 'Total bad alignability block size\t' + str(fragSizeSum)
    print 'Fraction of bad alignability block size to genome size (%)\t' + str(float(fragSizeSum) / float(genomeSize) * 100.0)

def getBlacklistCategories (bedfilepath):
    f = open(bedfilepath)
    cats = {}
    for line in f:
        toks = line.rstrip().split('\t')
        cat = toks[3]
        cats[cat] = 1
    f.close()
    print cats.keys()

def makeCombinedBed(filedir):
    wf = open(os.path.join(filedir, 'artifact.bed'), 'w')
    
    # Alignability 75mer
    f = open(os.path.join(filedir, 'align75.0.5.bed'))
    for line in f:
        toks = line.rstrip().split('\t')
        start = int(toks[1])
        stop = int(toks[2])
        bin = Bin.getBin(start, stop)
        wf.write(str(bin) + '\t' + '\t'.join(toks[:3]) + '\t' + 'A75\n')
    f.close()
    
    # DAC blacklist
    f = open(os.path.join(filedir, 'DAC_Blacklist.bed'))
    for line in f:
        toks = line.rstrip().split('\t')
        start = int(toks[1])
        stop = int(toks[2])
        bin = Bin.getBin(start, stop)
        wf.write(str(bin) + '\t' + '\t'.join(toks[:3]) + '\t' + blacklist_cats[toks[3]] + '\n')
    f.close()
    
    # Duke blacklist
    f = open(os.path.join(filedir, 'Duke_Blacklist.bed'))
    for line in f:
        toks = line.rstrip().split('\t')
        start = int(toks[1])
        stop = int(toks[2])
        bin = Bin.getBin(start, stop)
        wf.write(str(bin) + '\t' + '\t'.join(toks[:3]) + '\t' + blacklist_cats[toks[3]] + '\n')
    f.close()
    
    wf.close()

def makeBinExonFile ():
    wf = open(os.path.join(exondir, 'Transcript_Exon_Bin_Chr_cStart_cEnd.txt'), 'w')
    f = open(os.path.join(exondir, 'Transcript_Exon_Chr_cStart_cEnd.txt'))
    exons = {}
    for line in f:
        toks = line.rstrip().split('\t')
        chr = toks[0]
        start = int(toks[1])
        stop = int(toks[2])
        if exons.has_key(chr) == False:
            exons[chr] = {}
        bin = Bin.getBin(start, stop)
        if exons[chr].has_key(bin) == False:
            exons[chr][bin] = {}
        if exons[chr][bin].has_key(start) == False:
            wf.write(str(bin) + '\t' + '\t'.join(toks[:3]) + '\n')
            exons[chr][bin][start] = stop
    f.close()
    wf.close()
    
def getExonPercentage():
    import MySQLdb
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    
    exons = {}
    exomeSize = 0
    f = open('c:\\datasources\\snvbox\\Transcript_Exon_Bin_Chr_cStart_cEnd.txt')
    for line in f:
        [bin, chr, start, stop] = line.rstrip().split('\t')
        bin = int(bin)
        start = int(start)
        stop = int(stop)
        
        if exons.has_key(chr) == False:
            exons[chr] = {}
        if exons[chr].has_key(bin) == False:
            exons[chr][bin] = []
        exons[chr][bin].append((start, stop))
        
        size = stop - start + 1
        exomeSize += size
    f.close()
    print 'exome loaded. size =', exomeSize
    
    mappedExomeSize = 0
    f = open(os.path.join(mapdir, 'artifact.bed'))
    count = 0
    for line in f:
        
        count += 1
        if count % 100000 == 0:
            print count, 'mapped exome size\t' + str(mappedExomeSize)
            print count, '% mapped exome size\t' + str(float(mappedExomeSize) / float(exomeSize) * 100.0)
        
        [dummy, chr, start, stop, code] = line.rstrip().split('\t')
        
        if chrLens.has_key(chr):
            
            exonsChr = exons[chr]
            
            start = int(start)
            stop = int(stop)
            bins = Bin.getBin(start, stop, getall=True)
            
            hitCount = 0
            mappedSize = 0
            for bin in bins:
                if exonsChr.has_key(bin):
                    for (exonStart, exonStop) in exonsChr[bin]:
                        if start > exonStop or stop < exonStart:
                            continue
                        elif start <= exonStart:
                            if stop >= exonStop:
                                size = exonStop - exonStart + 1
                                if size > mappedSize:
                                    mappedSize = size
                                hitCount += 1
                            elif stop >= exonStart and stop <= exonStop:
                                size += stop - exonStart + 1
                                if size > mappedSize:
                                    mappedSize = size
                                hitCount += 1
                        elif start >= exonStart:
                            if stop <= exonStop:
                                size += stop - start + 1
                                if size > mappedSize:
                                    mappedSize = size
                                hitCount += 1
                            elif stop >= exonStop:
                                size += exonStop - start + 1
                                if size > mappedSize:
                                    mappedSize = size
                                hitCount += 1
#             if hitCount > 1:
#                 print '(',start,'~',stop,')', hitCount, 'hits. mapped size =', mappedSize
            
            mappedExomeSize += mappedSize
            
#             stmt = 'select start, stop from artifact where chr="' + chr + '" and bin in (' + ','.join([str(bin) for bin in bins]) + ')'
#             cursor.execute(stmt)
#             results = cursor.fetchall()
#             for result in results:
#                 (exonStart, exonStop) = result
#                 if start > exonStop or stop < exonStart:
#                     continue
#                 if start <= exonStart:
#                     if stop >= exonStop:
#                         mappedExomeSize += exonStop - exonStart + 1
#                     elif stop >= exonStart and stop <= exonStop:
#                         mappedExomeSize += stop - exonStart + 1
#                 elif start >= exonStart:
#                     if stop <= exonStop:
#                         mappedExomeSize += stop - start + 1
#                     elif stop >= exonStop:
#                         mappedExomeSize += exonStop - start + 1
    
    print 'mapped exome size\t' + str(mappedExomeSize)
    print '% mapped exome size\t' + str(float(mappedExomeSize) / float(exomeSize) * 100.0)

def createMySQLTable ():
    import MySQLdb
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    
    cursor.execute('drop table if exists artifact')
    db.commit()
    
    cursor.execute('create table artifact (bin int, chr varchar(5), start int, stop int, code varchar(3)) engine=innodb')
    db.commit()
    
    cursor.execute('load data local infile "/home/rkim/artifact.bed" into table artifact (bin, chr, start, stop, code)')
    db.commit()
    
    cursor.execute('create index artifact_idx on artifact (bin, chr)')
    db.commit()

# makeBinExonFile()
# findAndWriteBadRegions('c:\\projects\\cravat\\artifact\\align75', 0.5)
# getBadRegionPercent('c:\\projects\\cravat\\artifact\\align75.0.5.bed')
# getBlacklistCategories('c:\\projects\\cravat\\artifact\\Duke_Blacklist.bed')
# makeCombinedBed('c:\\projects\\cravat\\artifact')

# Before createMySQLTable, copy artifact.bed to web02:/home/rkim/artifact.bed
# Then, copy this script to web02:/home/rkim
# Then, run this script's createMySQLTable method.

# createMySQLTable()

getExonPercentage()