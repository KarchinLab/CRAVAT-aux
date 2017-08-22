import MySQLdb

mysql_host = 'karchin-db01.icm.jhu.edu'
mysql_user = 'mryan'
mysql_password = 'royalenfield'

def check_vogelstein_table():
    dbname = 'SNVBox_dev'
    db = MySQLdb.connect(host=mysql_host,\
                     user=mysql_user,\
                     passwd=mysql_password,\
                     db=dbname)
    cursor = db.cursor()

    genes = []
    f = open('vogelstein_drivers.txt')
    for line in f:
        genes.append(line.strip())
    f.close()
    
    for gene in genes:
        cursor.execute('select GeneSymbol from GeneSymbols where GeneSymbol="' + gene + '"')
        result = cursor.fetchone()
        print gene, len(result)
    
    # All gene symbols in vogelstein_drivers.txt exist in GeneSymbols table.

def get_cosmic_mutations_for_vogelstein_genes():
    dbname = 'CRAVAT_ANNOT_DEV'
    db = MySQLdb.connect(host=mysql_host,\
                     user=mysql_user,\
                     passwd=mysql_password,\
                     db=dbname)
    cursor = db.cursor()
    
    genes = []
    f = open('vogelstein_drivers.txt')
    for line in f:
        genes.append(line.strip())
    f.close()
    
    for gene in genes:
        cursor.execute('select chromosome, position, refbase, altbase from cosmic_genomic where genename="' + gene + '"')
        results = cursor.fetchall()

def process_cosmic_file(filename, wf_ref, wf_dup, cursor, count):
    print filename
    f = open(filename)
    for line in f:
        count += 1
        if count % 1000 == 0:
            print ' ', count
        [hugo, uid, accession, aa_pos, ref_aa, alt_aa, mutation_type, occurrences, chrom, pos1, pos2, pos3] = line[:-1].split('\t')
        wf_ref.write('\t'.join([str(count), hugo, uid, accession, aa_pos, ref_aa, alt_aa, mutation_type, occurrences]) + '\n')
        
        cursor.execute('select * from CodonTable where chrom="' + chrom + '" and pos1=' + pos1)
        results = cursor.fetchall()
        for result in results: # Includes the original accession.
            (other_uid, other_aa_pos, dummy, dummy, dummy, dummy, other_bases) = result
            other_uid = str(other_uid)
            other_aa_pos = str(other_aa_pos)
            cursor.execute('select * from Transcript where UID=' + other_uid)
            other_result = cursor.fetchone()
            (other_accession, other_ccds, other_refseqt, dummy, other_enst, dummy, dummy) = other_result
            if other_ccds != None:
                wf_dup.write('\t'.join([other_uid, other_ccds, other_aa_pos, ref_aa, alt_aa, mutation_type, str(count)]) + '\n')
                
            if other_refseqt != None:
                wf_dup.write('\t'.join([other_uid, other_refseqt, other_aa_pos, ref_aa, alt_aa, mutation_type, str(count)]) + '\n')
            
            if other_enst != None:
                wf_dup.write('\t'.join([other_uid, other_enst, other_aa_pos, ref_aa, alt_aa, mutation_type, str(count)]) + '\n')

    f.close()

    return count

def get_all_transcript_pos_for_vogelstein_cosmic_positions ():
    dbname = 'SNVBox_dev'
    db = MySQLdb.connect(host=mysql_host,\
                     user=mysql_user,\
                     passwd=mysql_password,\
                     db=dbname)
    cursor = db.cursor()
    
    wf_ref = open('vogelstein_cosmic_ref.txt', 'w')
    wf_dup = open('vogelstein_cosmic_dup.txt', 'w')
    
    count = 0
    count = process_cosmic_file('refseqt.txt', wf_ref, wf_dup, cursor, count)
    process_cosmic_file('enst.txt', wf_ref, wf_dup, cursor, count)

    wf_ref.close()
    wf_dup.close()
    
def make_vogelstein_cosmic_mysql_tables ():
    dbname = 'CRAVAT_ANNOT_DEV';
    db = MySQLdb.connect(host=mysql_host,\
                     user=mysql_user,\
                     passwd=mysql_password,\
                     db=dbname)
    cursor = db.cursor()

    vogelstein_cosmic_ref_table_name = 'vogelstein_cosmic_ref'
    cursor.execute('drop table if exists ' + vogelstein_cosmic_ref_table_name)
    db.commit()
    cursor.execute('create table ' + vogelstein_cosmic_ref_table_name + ' (id int not null, gene varchar(20) not null, UID int not null, accession varchar(20) not null, aa_pos int(11) not null, ref_aa varchar(40), alt_aa varchar(40), mutation_type varchar(4), occurrences int(11), primary key (id)) engine=innodb')
    db.commit()
    cursor.execute('load data local infile "vogelstein_cosmic_ref.txt" into table ' + vogelstein_cosmic_ref_table_name)
    db.commit()
    cursor.execute('create index vogelstein_cosmic_ref_idx1 on ' + vogelstein_cosmic_ref_table_name + ' (gene)')
    db.commit()

    vogelstein_cosmic_dup_table_name = 'vogelstein_cosmic_dup'
    cursor.execute('drop table if exists ' + vogelstein_cosmic_dup_table_name)
    db.commit()
    cursor.execute('create table ' + vogelstein_cosmic_dup_table_name + ' (UID int not null, accession varchar(20) not null, aa_pos int(11) not null, ref_aa varchar(40), alt_aa varchar(40), mutation_type varchar(4), ref_id int(11)) engine=innodb')
    db.commit()
    cursor.execute('load data local infile "vogelstein_cosmic_dup.txt" into table ' + vogelstein_cosmic_dup_table_name)
    db.commit()
    cursor.execute('create index vogelstein_cosmic_dup_idx1 on ' + vogelstein_cosmic_dup_table_name + ' (accession, aa_pos)')
    db.commit()
    
#get_all_transcript_pos_for_vogelstein_cosmic_positions()
make_vogelstein_cosmic_mysql_tables()
