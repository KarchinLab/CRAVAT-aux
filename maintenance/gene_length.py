import MySQLdb
import sys

mysql_host = 'karchin-db01.icm.jhu.edu'
mysql_user = 'mryan'
mysql_password = 'royalenfield'
db_name = 'hg19'
db = MySQLdb.connect(host=mysql_host,\
                     user=mysql_user,\
                     passwd=mysql_password,\
                     db=db_name)
cursor = db.cursor()

def compare_mark_and_my_gene_length ():
    f=open('d:\\cravat\\geman\\gencodeV12.geneCdsLen.tsv')
    mark = {}
    print 'MARK'
    for line in f:
        toks = line.rstrip().split('\t')
        gene = toks[1]
        gene_length = int(toks[2])
        if mark.has_key(gene):
            print 'duplicate len for', gene, mark[gene], gene_length
            if gene_length > mark[gene]:
                mark[gene] = gene_length
            print '#', mark[gene]
        else:
            mark[gene] = gene_length
    f.close()
    f=open('d:\\cravat\\geman\\gencode_gene_length.txt')
    mine = {}
    print 'MINE'
    for line in f:
        toks = line.rstrip().split('\t')
        gene = toks[0]
        gene_length = int(toks[1])
        if mine.has_key(gene):
            print 'duplicate len for',gene
        mine[gene] = gene_length
    print 'BOTH'
    mark_genes = mark.keys()
    mine_genes = mine.keys()
    wf = open('d:\\cravat\\geman\\mark_mine_gencode_compare.txt', 'w')
    for gene in mark_genes:
        mark_len = mark[gene]
        if mine.has_key(gene):
            mine_len = mine[gene]
        else:
            mine_len = ''
        wf.write(gene+'\t'+str(mark_len)+'\t'+str(mine_len)+'\n')
    for gene in mine_genes:
        if mark.has_key(gene) == False:
            wf.write(gene + '\t' + '' + '\t' + str(mine[gene]) + '\n')
    wf.close()

def get_coding_gencode_names ():
    stmt = 'select distinct geneName from wgEncodeGencodeAttrsV12 ' + \
           'where transcriptClass="coding"'
    cursor.execute(stmt)
    results = cursor.fetchall()
    wf = open('coding_genename_gencode.txt', 'w')
    for result in results:
        wf.write(result[0]+'\n')
    wf.close()


def gencode_gene_length ():
    stmt = 'select g.geneName, c.name, c.chrom, c.strand, ' + \
           'c.cdsStart, c.cdsEnd, c.exonCount, c.exonStarts, c.exonEnds ' + \
           'from wgEncodeGencodeAttrsV12 as g, wgEncodeGencodeCompV12 as c ' + \
           'where g.transcriptClass="coding" and g.transcriptId = c.name'
    cursor.execute(stmt)
    results = cursor.fetchall()
    gene_exoninfo = {}
    for row in results:
    #    print row
        [symbol, transcript, chrom, strand, \
         cds_start, cds_end, exon_count, exon_start_str, exon_end_str] = row
        cds_start = int(cds_start)
        cds_end = int(cds_end)
        '''UCSC Genome database's all "start" is 0-based and "end" is 1-based'''
        exon_starts = [int(v) for v in exon_start_str.split(',') if v != '']
        exon_ends = [int(v) for v in exon_end_str.split(',') if v != '']
        cds_len = 0
        for exon_no in xrange(len(exon_starts)):
            exon_len = 0
            exon_start = exon_starts[exon_no]
            exon_end = exon_ends[exon_no]
            if cds_start >= exon_start and cds_end <= exon_end:
                exon_len = cds_end - cds_start
            elif cds_start >= exon_start and cds_start <= exon_end:
                exon_len = exon_end - cds_start
            elif cds_end >= exon_start and cds_end <= exon_end:
                exon_len = cds_end - exon_start
            elif exon_start > cds_end:
                continue
            elif exon_end < cds_start:
                continue
            else:
                exon_len = exon_end - exon_start
            cds_len += exon_len
#        if cds_len % 3 != 0:
#            print 'cds_len error. gene=', symbol, ', cds_len=', cds_len
#            print row
        if gene_exoninfo.has_key(symbol) == False:
            gene_exoninfo[symbol] = 0
        if cds_len > gene_exoninfo[symbol]:
            gene_exoninfo[symbol] = cds_len
    wf = open('d:\\cravat\\geman\\gencode_gene_length.txt', 'w')    
    for symbol in gene_exoninfo.keys():
        wf.write(symbol+'\t'+str(gene_exoninfo[symbol])+'\n')
    wf.close()

def examine_gene_length_overlap (gencode_filename, \
                                 refseq_filename, \
                                 compare_filename, \
                                 gencode_only_filename, \
                                 refseq_only_filename):
    f = open(gencode_filename)
    gencode={}
    for line in f:
        toks = line.rstrip().split('\t')
        if len(toks) == 2:
            gencode[toks[0]] = int(toks[1])
        elif len(toks) == 3:
            gencode[toks[1]] = int(toks[2])
    f.close()
    f=open(refseq_filename)
    refgene={}
    for line in f:
        toks = line.rstrip().split('\t')
        refgene[toks[0]] = int(toks[2])
    f.close()
    gencode_keys = gencode.keys()
    refgene_keys = refgene.keys()
    no_not_in_refgene = 0
    no_not_in_gencode = 0
    no_common = 0
    wf = open(compare_filename, 'w')
    wf_gencode_only = open(gencode_only_filename, 'w')
    wf_refseq_only  = open(refseq_only_filename, 'w')
    for symbol in gencode_keys:
        gencode_len = gencode[symbol]
        if not symbol in refgene_keys:
            refgene_len = ''
            dif = ''
            no_not_in_refgene += 1
            wf_gencode_only.write(symbol + '\t' + str(gencode_len) + '\n')
        else:
            refgene_len = refgene[symbol]
            dif = gencode_len - refgene_len
            no_common += 1
            wf.write(symbol + '\t' + str(gencode_len) + '\t' + str(refgene_len) + '\t' + str(dif) + '\n')
    for symbol in refgene_keys:
        refgene_len = refgene[symbol]
        if not symbol in gencode_keys:
            gencode_len = ''
            dif = ''
            no_not_in_gencode += 1
#            wf.write(symbol + '\t' + str(gencode_len) + '\t' + str(refgene_len) + '\t' + str(dif) + '\n')
            wf_refseq_only.write(symbol + '\t' + str(refgene_len) + '\n')
    wf.close()
    wf_gencode_only.close()
    wf_refseq_only.close()
    print len(gencode), 'genCode genes'
    print len(refgene), 'refGene genes'
    print no_common,'genes both in GenCode and refGene'
    print 'Not in RefGene' + '\t' + str(no_not_in_refgene) + '\tgenes'
    print 'Not in GenCode' + '\t' + str(no_not_in_gencode) + '\tgenes'

def make_gencode_refgene_gene_length_txt ():
    f = open('d:\\cravat\\cravat\\webcontent\\wrappers\\gene_length.txt')
    refgene = {}
    for line in f:
        toks = line.rstrip().split('\t')
        refgene[toks[0]] = int(toks[2])
    f.close()
    f = open('d:\\cravat\\geman\\gencode_gene_length.txt')
    gencode = {}
    for line in f:
        toks = line.rstrip().split('\t')
        gencode[toks[0]] = int(toks[1])
    f.close()
    gene_lengths = {}
    gencode_genes = gencode.keys()
    refgene_genes = refgene.keys()
    for gene in gencode_genes:
        gene_lengths[gene] = gencode[gene]
    for gene in refgene_genes:
        if gene_lengths.has_key(gene) == False:
            gene_lengths[gene] = refgene[gene]
    wf = open('d:\\cravat\\geman\\gene_lengths_gencode_refgene.txt', 'w')
    for gene in gene_lengths.keys():
        wf.write(gene + '\t' + 'NA' + '\t' + str(gene_lengths[gene]) + '\n')
    wf.close()

#gencode_gene_length()
#compare_mark_and_my_gene_length()
#examine_gene_length_overlap('d:\\cravat\\geman\\gencodeV12.geneCdsLen.tsv' , \
#                            'd:\\cravat\\cravat\\webcontent\\wrappers\\gene_length.txt', \
#                            'd:\\cravat\\geman\\gencode_refseq_compare_mark.txt', \
#                            'd:\\cravat\\geman\\gencode_only_mark.txt', \
#                            'd:\\cravat\\geman\\refseq_only_mark.txt')
#examine_gene_length_overlap('d:\\cravat\\geman\\gencode_gene_length.txt' , \
#                            'd:\\cravat\\cravat\\webcontent\\wrappers\\gene_length.txt', \
#                            'd:\\cravat\\geman\\gencode_refseq_compare_mine.txt', \
#                            'd:\\cravat\\geman\\gencode_only_mine.txt', \
#                            'd:\\cravat\\geman\\refseq_only_mine.txt')
make_gencode_refgene_gene_length_txt()