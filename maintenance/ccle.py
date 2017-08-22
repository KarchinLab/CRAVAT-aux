import MySQLdb
import sys

def convert_to_cravat_format_step_1 ():
    f = open('C:\\Projects\\CRAVAT\\CCLE\\CCLE_hybrid_capture1650_hg19_NoCommonSNPs_NoNeutralVariants_CDS_2012.05.07.maf')
    
    # Gets necessary headers columns and their positions
    toks = f.readline().split('\t')
    field_pos = {}
    sel_fields = ['Hugo_Symbol', 
                  'Entrez_Gene_Id', 
                  'Chromosome', 
                  'Start_position', 
                  'End_position', 
                  'Strand', 
                  'Reference_Allele', 
                  'Tumor_Seq_Allele1', 
                  'Tumor_Seq_Allele2', 
                  'Tumor_Sample_Barcode', 
                  'Genome_Change', 
                  'Annotation_Transcript', 
                  'Transcript_Strand', 
                  'cDNA_Change', 
                  'Codon_Change', 
                  'Protein_Change', 
                  'Refseq_mRNA_Id'
                  ]
    for i in xrange(len(toks)):
        field_name = toks[i]
        if field_name in sel_fields:
            field_pos[field_name] = i
    
    wf = open('C:\\Projects\\CRAVAT\\CCLE\\CCLE_hybrid_capture1650_cravat_intermediate.txt', 'w')
    line_no = 0
    for line in f:
        line_no += 1
        toks = line.split('\t')
        chrom = toks[field_pos['Chromosome']]
        pos = toks[field_pos['Start_position']]
        strand = toks[field_pos['Strand']]
        ref = toks[field_pos['Reference_Allele']]
        alt_1 = toks[field_pos['Tumor_Seq_Allele1']] # Tumor_Seq_Allel2 is the same as Tumor_Seq_Allel1
        sample = toks[field_pos['Tumor_Sample_Barcode']]
        wf.write('\t'.join([str(line_no), 'chr' + chrom, pos, strand, ref, alt_1, sample]) + '\n')
    
    f.close()
    wf.close()

def count_variants_by_cell ():
    f = open('c:\\projects\\cravat\\cravat\\maintenance\\target_list.txt')
    target_genes = {}
    for line in f:
        target_genes[line.split('\t')[0]] = True
    f.close()
    
    f = open('C:\\Projects\\CRAVAT\\CCLE\\ccle.info.txt')
    f.readline() # Skips the header line.
    variants = {} # Key is cell-line.
    for line in f:
        [uid, chrom, pos, strand, ref, alt, cell, hugo, transcript, dummy, dummy, aa_pos, so_term, ref_aa, alt_aa, other_transcripts] = line[:-1].split('\t')
        if target_genes.has_key(hugo):
            if variants.has_key(cell) == False:
                variants[cell] = {}
            var_str = chrom + ':' + pos + '_' + ref + '_' + alt
            variants[cell][var_str] = True
    f.close()
    
    print len(variants), 'celllines'
    for cell in variants.keys():
        print len(variants[cell])

def make_mysql_file():
    f = open('C:\\Projects\\CRAVAT\\CCLE\\ccle.info.txt')
    f.readline()
    wf = open('c:\\projects\\cravat\\ccle\\ccle.formysql.txt', 'w')
    for line in f:
        [uid, chrom, pos, strand, ref, alt, cell, hugo, transcript, dummy, dummy, aa_pos, so_term, ref_aa, alt_aa, other_transcripts] = line[:-1].split('\t')
        if hugo != 'No HUGO symbol found':
            if so_term == 'II' or so_term == 'ID':
                so_term = 'IV'
            elif so_term == 'FI' or so_term == 'FD':
                so_term = 'FV'
            wf.write('\t'.join([transcript, '1', so_term, aa_pos, ref_aa, alt_aa, cell]) + '\n')
            if other_transcripts != '':
                for change in other_transcripts.split(','):
                    [transcript2, dummy, aa_change2] = change.strip().split(':')
                    try:
                        lb_pos = aa_change2.index('(')
                        [aa_pos2, so_term2] = aa_change2.split('(')
                        so_term2 = so_term2.split(')')[0]
                        if so_term2 == 'II' or so_term2 == 'ID':
                            so_term2 = 'IV'
                        elif so_term2 == 'FI' or so_term2 == 'FD':
                            so_term2 = 'FV'
                        ref_aa2 = ''
                        alt_aa2 = ''
                    except Exception, e:
                        ref_aa2 = aa_change2[0]
                        alt_aa2 = aa_change2[-1]
                        aa_pos2 = aa_change2[1:-1]
                        if ref_aa2 == '*' and alt_aa2 != '*':
                            so_term2 = 'SL'
                        elif ref_aa2 != '*' and alt_aa2 == '*':
                            so_term2 = 'SG'
                        elif ref_aa2 != alt_aa2:
                            so_term2 = 'MS'
                        else:
                            so_term2 = 'SY'
                    wf.write('\t'.join([transcript2, '0', so_term2, aa_pos2, ref_aa2, alt_aa2, cell]) + '\n')
    f.close()
    wf.close()

def make_table():
    db = MySQLdb.connect(host='karchin-db01.icm.jhu.edu', user='mryan', passwd='royalenfield', db='CRAVAT_ANNOT_DEV')
    cursor = db.cursor()
    cursor.execute('drop table if exists ccle')
    db.commit()
    cursor.execute('create table ccle (transcript varchar(30), representative bit, so varchar(3), pos int, ref varchar(100), alt varchar(100), cell varchar(50)) engine=innodb')
    db.commit()
    cursor.execute('load data local infile "ccle.formysql.txt" into table ccle')
    db.commit()

def make_drug_data_mysql_file():
    wf = open('drug_data_mysql.txt', 'w')
    
    f = open('C:\\Projects\\CRAVAT\\CCLE\\drug_data.txt')
    f.readline()
    for line in f:
        toks = line[:-1].split('\t')
        cell = toks[0]
        drug = toks[2]
        target = toks[3]
        ec50 = toks[9]
        ic50 = toks[10]
        
        if ec50 == 'NA':
            ec50 = 99999.0
        else:
            ec50 = float(ec50)
        if ic50 == 'NA':
            ic50 = 99999.0
        else:
            ic50 = float(ic50)
        
        if ec50 < 1.0:
            effective = 1
        else:
            effective = 0
        
        wf.write(cell + '\t' + drug + '\t' + target + '\t' + str(ec50) + '\t' + str(ic50) + '\t' + str(effective) + '\n')
    
    wf.close()

def make_drug_data_table():
    db = MySQLdb.connect(host='karchin-db01.icm.jhu.edu', user='mryan', passwd='royalenfield', db='CRAVAT_ANNOT_DEV')
    cursor = db.cursor()
    cursor.execute('drop table if exists ccle_drug')
    db.commit()
    cursor.execute('create table ccle_drug (cell varchar(50), drug varchar(20), target varchar(10), ec50 double, ic50 double, effective tinyint(1)) engine=innodb')
    db.commit()
    cursor.execute('load data local infile "drug_data_mysql.txt" into table ccle_drug')
    db.commit()
    cursor.execute('create index ccle_idx1 on ccle (transcript, pos, ref, alt)')
    db.commit()
    cursor.execute('create index ccle_idx2 on ccle (transcript, pos)')
    db.commit()
    
#make_mysql_file()
#make_drug_data_mysql_file()
make_drug_data_table()