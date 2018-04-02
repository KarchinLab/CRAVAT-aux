import sqlite3
import os
import re
import sys
import vcf
import paramiko

"""
The latest update will be v84
"""

"""
COSMIC data to download: VCF Files (coding and non-coding mutations), COSMIC Mutation Data
"""

"""
COSMIC_Processor class is used to read in a COSMIC database file and
generate three sqlite tables for the COSMIC data in "CRAVAT_ANNOT_DEV" database
in karchin-db01.icm.jhu.edu. The three sqlite tables correspond to
genomic, amino acid-, and tissue- level summary of the COSMIC data.
"""

#
# ALL DATA IS CONVERTED TO (+) STRAND.
#

# COSMIC_genomic table uses position, refbase, and altbase. For insertion,
# c.1616_1617insGCA is an example of COSMIC data, which shows the insertion
# of GCA between the 1616th and 1617th bases. VCF format would show this 
# same information as position 1616, refbase x where x is the actual 
# reference base at the 1616th position, and altbase as xGCA. However,
# COSMIC data does not have the reference base at the 1616th position.
# Thus, let's have the CRAVAT format to be:
#
# chrX 1617 . GCA            DEREK. WE USE - NOW INSTEAD OF . RIGHT!!!
#
# where '.' is the same as ' '.
#
# COSMIC v67 has both + and - strands.
#
class COSMIC_Processor:
    aas = ['A', 'C', 'D', 'E', 'F', \
           'G', 'H', 'I', 'K', 'L', \
           'M', 'N', 'P', 'Q', 'R', \
           'S', 'T', 'V', 'Y', 'W']
    base_rev_dic = {'A':'T', 'T':'A', 'G':'C', 'C':'G', 'X':'X', '.':'.', 'N':'N'}
    
    def __init__ (self):
        self.genomic_sqlite_filename = 'cosmic_genomic_sqlite.txt'
        self.accession_sqlite_filename = 'cosmic_accession_sqlite.txt'
        self.genomic_table = 'cosmic_genomic'
        self.accession_table = 'cosmic_accession'
        self.rep_limit = 100

    def main (self, vcf_coding_cosmic_filename, vcf_noncoding_cosmic_filename, tsv_cosmic_mutants_filename):
        self.vcf_coding_cosmic_filename = vcf_coding_cosmic_filename
        self.vcf_noncoding_cosmic_filename = vcf_noncoding_cosmic_filename
        self.tsv_cosmic_mutants_filename = tsv_cosmic_mutants_filename
        self.tmp_file_dir = os.path.dirname(vcf_coding_cosmic_filename)
        print('make_db_files')
        self.make_sqlite_files()
        print('make_db_tables')
        self.make_sqlite_tables()
        
    def get_reverse_strand_bases (self, bases): # reverses bases in the reverse order
        rev_bases = ''
        for base_pos in range(len(bases)-1,-1,-1):
            rev_bases += self.base_rev_dic[bases[base_pos]]
        return rev_bases
    
    def report_major_error_vcf(self, message, uid, alts, AA_change, num_entry):
        sys.stderr.write(str(message) + '\n')
        sys.stderr.write('num_entry = ' + str(num_entry) +\
                         '    cosmic id = ' + str(uid) + \
                         '    alts = ' + str(alts) + \
                         '     AA_change =  ' + str(AA_change) + \
                         '   fix!!!!!\n\n')

    def report_major_error_tsv(self, message, line_num, uid, old_accession, new_accession):
        sys.stderr.write(str(message) + '\n')
        sys.stderr.write('line_num = ' + str(line_num) + \
                         '    cosmic id = ' + str(uid) + \
                         '    old_accession = ' + str(old_accession) + \
                         '    new_accession =  ' + str(new_accession) + \
                         '   fix!!!!!\n\n')

    def trimming_vcf_input(self, ref, alt, pos, strand):
        pos = int(pos)
        
        reflen = len(ref)
        altlen = len(alt)
        minlen = min(reflen, altlen)
        
        new_ref = ref
        new_alt = alt
        new_pos = pos
    
        # Trims from the end. Except don't remove the first nucleotide. 1:6530968 CTCA -> GTCTCA becomes C -> GTC.
        for nt_pos in range(0, minlen - 1): 
            if ref[reflen - nt_pos - 1] == alt[altlen - nt_pos - 1]:
                new_ref = ref[:reflen - nt_pos - 1]
                new_alt = alt[:altlen - nt_pos - 1]
            else:
                break    
                
        new_ref_len = len(new_ref)
        new_alt_len = len(new_alt)
        
        minlen = min(new_ref_len, new_alt_len)
        
        new_ref2 = new_ref
        new_alt2 = new_alt
        
            
        # Trims from the start. 1:6530968 G -> GT becomes 1:6530969 - -> T.
        for nt_pos in range(0, minlen):
            if new_ref[nt_pos] == new_alt[nt_pos]:
                if strand == '+':
                    new_pos += 1
                elif strand == '-':
                    new_pos -= 1
                new_ref2 = new_ref[nt_pos + 1:]
                new_alt2 = new_alt[nt_pos + 1:]
            else:
                new_ref2 = new_ref[nt_pos:]
                new_alt2 = new_alt[nt_pos:]
                break  
            
        return new_ref2, new_alt2, new_pos
    
    def make_sqlite_files (self):
        print('Extracting from cosmic files:')
        #READ THE VCF CODING MUTATIONS FILE
        #     Build dictionary
        #         Key:
        #             cosmic_id
        #         Values:
        #             - chromosome
        #             - position
        #             - refbase
        #             - altbase
        #             - genename
        #             - aachange_cosmic
        cosmic_data_vcf_coding = self.extract_from_vcf(self.vcf_coding_cosmic_filename, 'coding')
        
        #READ THE VCF NON-CODING MUTATIONS FILE   
        # WE DO NOT CURRENTLY SUPPORT THIS!!!     
        #         cosmic_data_vcf_NonCoding = self.extract_from_vcf(self.vcf_noncoding_cosmic_filename, 'noncoding')
        
        #READ THE TSV MUTATIONS FILE
        #     Build dictionary
        #         Key:
        #             cosmic_id
        #         Values:
        #             - accession
        #             - occurrences
        #             - primarysites
        #             - primarysitenos
        cosmic_data_tsv_mutants = self.extract_from_tsv(self.tsv_cosmic_mutants_filename)        

        # Using the dictionaries from the VCF file and the TSV file make the sqlite input files
        #     -cosmic_accession_sqlite.txt
        #     -cosmic_genomic_sqlite.txt
        print('Making sqlite input files:')
        self.make_sqlite_file_for_comsic_genomic(cosmic_data_vcf_coding, cosmic_data_tsv_mutants)
        self.make_sqlite_file_for_cosmic_accession(cosmic_data_vcf_coding, cosmic_data_tsv_mutants)
         
        # Make the cosmic sqlite tables and fill them using the cosmic sqlite files previously created. This uses global variables to the class
        #     -CRAVAT_ANNOT_DEV
        #         -cosmic_genomic
        #         -cosmic_accession
        print('Making the sqlite tables')
        self.make_sqlite_tables()
        
        return
        
    def extract_from_vcf(self, vcf_file, coding_or_non):
        print('\t' + str(vcf_file))
        
        cosmic_vcf_data = {}
        
        rf_vcf_cosmic = open(vcf_file, 'rU')
        
        num_entry = 0
        for entry in vcf.Reader(rf_vcf_cosmic):        #First looking at every VCF entry
            num_entry += 1
            if num_entry % 100000 == 0:
                print('\t\t' + str(num_entry))
            id = entry.ID
            chr = entry.CHROM
            pos = entry.POS
            ref = entry.REF
            alts = entry.ALT
            gene_name = None
            if 'GENE' in entry.INFO:
                gene_name = entry.INFO['GENE']
            else:
                gene_name = ''
                sys.stderr.write("no GENE!!" + num_entry + '\n')
                sys.exit(-1)
                
            #             If a gene_name contains an underscor '_'. Then don't include that gene in cosmic
            if '_' in gene_name:
                continue
            
            cosmic_AA_change = None
            if 'AA' in entry.INFO:
                cosmic_AA_change =  entry.INFO['AA']
            else:
                cosmic_AA_change =  ''
                sys.stderr.write("no AA_change!!" + num_entry + '\n')
                sys.exit(-1)            
            #             Check if the id already exists in the dictionary cosmic_data
            if id in cosmic_vcf_data:
                self.report_major_error_vcf('THE COSMIC ID OCCURS MORE THAN ONCE!!! NO!!! THIS RUINS YOUR CODE!!!! num_entry is the second time!!!', id, alts, '', num_entry)
                rf_vcf_cosmic.close()
                sys.exit(-1)
            else:
                cosmic_vcf_data[id] = {}
            
            #             Check if there is more than one alternate base in the alts location
            if len(alts) > 1:
                self.report_major_error_vcf("ALTS HAS MORE THAN ONE ENTRY!!! YOU NEED TO CHANGE YOUR CODE!!!!", id, alts, cosmic_AA_change, num_entry)
                rf_vcf_cosmic.close()
                sys.exit(-1)
            alt = str(alts[0])
            
            #             Trim the ref and alt. Can change the ref, alt, and position
            #             You are looking at vcf format so you can put in that you are looking at the + strand
            new_ref, new_alt, new_pos = self.trimming_vcf_input(ref, alt, pos, '+')
            if new_ref == '':
                new_ref = '-'
            if new_alt == '':
                new_alt = '-'            
            
            #       determine the sequence ontology / mutation type
            mutation_type = None
            if coding_or_non == 'coding':
                new_ref_len = len(new_ref)
                new_alt_len = len(new_alt)
                if new_ref == '-' and new_alt != '-': # Insertion
                    if new_alt_len % 3 == 0:
                        mutation_type = 'ii'
                    else:
                        mutation_type = 'fi'
                elif new_ref != '-' and new_alt == '-': # Deletion
                    if new_ref_len % 3 == 0:
                        mutation_type = 'id'
                    else:
                        mutation_type = 'fd'
                elif new_ref_len == 1 and new_alt_len == 1: # SNP
                    mutation_type = 'snp'
                else:
                    mutation_type = 'cs'
            elif coding_or_non == 'noncoding':
                mutation_type = 'noncoding'

            cosmic_vcf_data[id]['chr'] = chr
            cosmic_vcf_data[id]['pos'] = new_pos
            cosmic_vcf_data[id]['ref'] = new_ref
            cosmic_vcf_data[id]['alt'] = new_alt
            cosmic_vcf_data[id]['gene'] = gene_name
            cosmic_vcf_data[id]['AA_change'] = cosmic_AA_change
            cosmic_vcf_data[id]['mutation_type'] = mutation_type

        
        rf_vcf_cosmic.close()
        
        return cosmic_vcf_data
    
    # This function will extract information from the CosmicMutantExport.tsv file.
    #     Builds a dictionary
    #         Key:
    #             cosmic_id
    #         Values:
    #             - accession
    #             - occurrences
    #             - primarysites
    #             - primarysitenos
    def extract_from_tsv(self, tsv_file):
        print('\t' + str(tsv_file))
        cosmic_tsv_data = {}
        
        rf_tsv = open(tsv_file, 'rU')
        
        num_variant = 0
        line_num = 0
        titles = []
        for line in rf_tsv:
            line_num += 1
            line = remove_new_line_character(line)
            toks = line.split('\t')
            if line_num == 1:
                titles = toks
            else:
                num_variant += 1
                if num_variant % 100000 == 0:
                    print('\t\t' + str(num_variant))
                line_dict = make_dictionary_of_titles_and_line_tabs(titles, toks)
                                
                cosmic_id = line_dict['Mutation ID']
                accession = line_dict['Accession Number']
                idsample = line_dict['ID_sample']
                
                if cosmic_id in cosmic_tsv_data:
                    if accession != cosmic_tsv_data[cosmic_id]['accession']:
                        self.report_major_error_tsv('THE ID HAS MORE THAN ONE ACCESSION NUMBER!!!! THIS IS BAD!!!!', line_num, cosmic_id, cosmic_tsv_data[cosmic_id]['accession'], accession)
                else:
                    cosmic_tsv_data[cosmic_id] = {}
                    cosmic_tsv_data[cosmic_id]['accession'] = accession
                    cosmic_tsv_data[cosmic_id]['occurrences'] = 0
                    cosmic_tsv_data[cosmic_id]['primary_sites'] = {}
                    cosmic_tsv_data[cosmic_id]['idsamples'] = {}
                if idsample in cosmic_tsv_data[cosmic_id]['idsamples']:
                    continue
                else:
                    cosmic_tsv_data[cosmic_id]['idsamples'][idsample] = True
                primary_site_string =  line_dict['Primary site']
                primary_sites = primary_site_string.split(',')
                for primary_site in primary_sites:
                    if primary_site in cosmic_tsv_data[cosmic_id]['primary_sites']:
                        cosmic_tsv_data[cosmic_id]['primary_sites'][primary_site] += 1
                        cosmic_tsv_data[cosmic_id]['occurrences'] += 1
                    else:
                        cosmic_tsv_data[cosmic_id]['primary_sites'][primary_site] = 1
                        cosmic_tsv_data[cosmic_id]['occurrences'] += 1
                    
        rf_tsv.close()
        
        return cosmic_tsv_data
 
    #     function makes the sqlite file that will be used to fill the cosmic_genomic table in the CRAVAT_ANNOT_DEV database 
    def make_sqlite_file_for_comsic_genomic(self, vcf_coding_dict, tsv_dict):
        print('\t' + self.genomic_sqlite_filename)
        wf_cosmic_genomic = open(os.path.join(self.tmp_file_dir, self.genomic_sqlite_filename), 'w')
        for id in vcf_coding_dict:
            cosmic_id_info = []
            cosmic_id_info.append(id)
            cosmic_id_info.append(str(vcf_coding_dict[id]['gene']))
            cosmic_id_info.append(str(tsv_dict[id]['accession']))
            cosmic_id_info.append('')       #This is a filler for aachange. Which we do not use anymore
            cosmic_id_info.append(str(vcf_coding_dict[id]['AA_change']))
            cosmic_id_info.append('chr' + str(vcf_coding_dict[id]['chr']))
            cosmic_id_info.append(str(vcf_coding_dict[id]['pos']))
            cosmic_id_info.append(str(vcf_coding_dict[id]['mutation_type']))
            cosmic_id_info.append(str(vcf_coding_dict[id]['ref']))
            cosmic_id_info.append(str(vcf_coding_dict[id]['alt']))
            cosmic_id_info.append(str(tsv_dict[id]['occurrences']))
            
            primarysites, primarysitesnos = self.make_string_for_primary_sites_input(tsv_dict[id]['primary_sites'])
            
            cosmic_id_info.append(str(primarysites))
            cosmic_id_info.append(str(primarysitesnos))
            
            wf_cosmic_genomic.write('\t'.join(cosmic_id_info) + '\n')
        
        wf_cosmic_genomic.close()
        
        return
    
    #     function makes the sqlite file that will be used to fill the cosmic_accession table in the CRAVAT_ANNOT_DEV database
    def make_sqlite_file_for_cosmic_accession(self, vcf_coding_dic, tsv_dic):
        print('\t' + self.accession_sqlite_filename)
        
        cosmic_accession_dict = self.make_cosmic_accession_dict(vcf_coding_dic, tsv_dic)
        wf_cosmic_accession = open(os.path.join(self.tmp_file_dir,  self.accession_sqlite_filename), 'w')
        for gene in cosmic_accession_dict:
            cosmic_accession_info = []
            cosmic_accession_info.append(gene)
            cosmic_accession_info.append(cosmic_accession_dict[gene]['accession'])
            cosmic_accession_info.append(str(cosmic_accession_dict[gene]['occurrences']))
            
            primarysites, primarysitesnos = self.make_string_for_primary_sites_input(cosmic_accession_dict[gene]['primary_sites'])
            cosmic_accession_info.append(primarysites)
            cosmic_accession_info.append(primarysitesnos)
            
            cosmic_ids_string = ';'.join(cosmic_accession_dict[gene]['cosmic_ids'])
            cosmic_accession_info.append(cosmic_ids_string)
            
            cosmic_accession_info.append(str(cosmic_accession_dict[gene]['truncated_cosmic_ids']))
            
            wf_cosmic_accession.write('\t'.join(cosmic_accession_info) + '\n')
        wf_cosmic_accession.close()
        
        return

    def make_string_for_primary_sites_input(self, primary_sites_dict):
        primarysites = ''
        primarysitesnos = ''
        num_sites_written = 0
        for primesite in primary_sites_dict:
            if num_sites_written > 0:
                primarysites += ';'
                primarysitesnos += ';'
            primarysites += primesite
            primarysitesnos += str(primary_sites_dict[primesite])
            num_sites_written += 1        
        return primarysites, primarysitesnos
 
    def make_cosmic_accession_dict(self, vcf_coding_dic, tsv_dic):
        cosmic_accession_dic = {}
        num_cosmic_ids_for_gene = {}
        
        for id in vcf_coding_dic:
            gene = vcf_coding_dic[id]['gene']
            accession = tsv_dic[id]['accession']
            occurrences = tsv_dic[id]['occurrences']
            primary_sites = tsv_dic[id]['primary_sites']
            if gene not in cosmic_accession_dic:
                cosmic_accession_dic[gene] = {}
                cosmic_accession_dic[gene]['accession'] = accession
                cosmic_accession_dic[gene]['cosmic_ids'] = []
                cosmic_accession_dic[gene]['cosmic_ids'].append(id)
                num_cosmic_ids_for_gene[gene] = 1
                cosmic_accession_dic[gene]['truncated_cosmic_ids'] = 'N'
                cosmic_accession_dic[gene]['occurrences'] = occurrences
                cosmic_accession_dic[gene]['primary_sites'] = {}
                self.put_primary_sites_in_accession_dict(primary_sites, cosmic_accession_dic[gene]['primary_sites'])
            else:
                if accession != cosmic_accession_dic[gene]['accession']:
                    print(gene, cosmic_accession_dic[gene], 'does not equal', accession)
                num_cosmic_ids_for_gene[gene] += 1
#                 We are limiting to only 1000 cosmic ids for a gene
                if num_cosmic_ids_for_gene[gene] <= 1000:
                    cosmic_accession_dic[gene]['cosmic_ids'].append(id)
                else:
                    cosmic_accession_dic[gene]['truncated_cosmic_ids'] = 'Y'           #This will happen everytime even though it only needs to once
                cosmic_accession_dic[gene]['occurrences'] += occurrences
                self.put_primary_sites_in_accession_dict(primary_sites, cosmic_accession_dic[gene]['primary_sites'])
        
        return cosmic_accession_dic
    
    def put_primary_sites_in_accession_dict(self, primary_sites, cosmic_accession_gene_primeSite_dict):
        for site in primary_sites:
            if site in cosmic_accession_gene_primeSite_dict:
                cosmic_accession_gene_primeSite_dict[site] += primary_sites[site]
            else:
                cosmic_accession_gene_primeSite_dict[site] = primary_sites[site]
        return
    
    def make_sqlite_tables (self):
        print('Entered make_sqlite_tables')
        db = sqlite3.connect('cosmic.sqlite')
        cursor = db.cursor()
        print('db and cursor gotten')
        print('dropping '+self.genomic_table)
        cursor.execute('drop table if exists '+self.genomic_table)
        print('creating '+self.genomic_table)
        cursor.execute('create table '+self.genomic_table+' (' +\
                       'cosmic_id text, '+\
                       'genename texr, '+\
                       'accession text, '+\
                       'aachange text, '+\
                       'aachange_cosmic text, '+\
                       'chromosome text, '+\
                       'position int, '+\
                       'mutation_type text, '+\
                       'refbase text, '+\
                       'altbase text, '+\
                       'occurrences int, '+\
                       'primarysites text, '+\
                       'primarysitenos text'+\
                       ')')
        print(self.genomic_table, 'generated')
        
        # Import afterwards.
        
        cursor.execute('create index '+self.genomic_table+'_index on '+self.genomic_table+'(chromosome, position)')
        print(self.genomic_table, 'indexed')
        db.commit()
        cursor.close()
        db.close()
        
        db = sqlite3.connect('cosmic_gene.sqlite')
        cursor = db.cursor()
        cursor.execute('drop table if exists '+self.accession_table)
        cursor.execute('create table '+self.accession_table+' (' +\
                       'genename varchar(45), '+\
                       'accession varchar(45), '+\
                       'occurrences int, '+\
                       'primarysites varchar(500), '+\
                       'primarysitenos varchar(200), '+\
                       'cosmic_ids varchar(12000), '+\
                       'truncated_cosmic_ids varchar(1)' +\
                       ')')
        
        # Import afterwards.
        
        cursor.execute('create index '+self.accession_table+'_index on '+self.accession_table+'(accession)')
        print(self.accession_table, 'indexed')
        db.commit()
        cursor.close()
        db.close()

def remove_new_line_character (line):
    line = line.rstrip('\r\n')
    return line

def determine_if_start_reading_cravat_output_file(line, file_type):
    start_reading = False
    if file_type == 'tsv':
        if line != '':
            if line[0] != '#':
                start_reading = True
    elif file_type == 'vcf':
        toks = line.split("\t")
        first_col =  toks[0]
        if first_col == "#CHROM":
            start_reading = True
    return start_reading

def make_dictionary_of_titles_and_line_tabs (titles, toks):
    line_dict = {}
    tok_num = 0
    for tok in toks:
        line_dict[titles[tok_num]] = tok
        tok_num += 1
    return line_dict

def check_first_and_second_gene_and_strand_INFO_in_VCF_match(cosmic_file):
    wf = open(cosmic_file, 'rU')
    start_reading = False
    line_num = 0
    for line in wf:
        line_num += 1
        if line_num % 100000 == 0:
            print(line_num)
        line = remove_new_line_character(line)
        if start_reading == False:
            start_reading = determine_if_start_reading_cravat_output_file(line, 'vcf')
            continue
        toks = line.split('\t')
        info = toks[7]
        info_pieces = info.split(';')
        
        gene1 = None
        strand1 = None
        gene2 = None
        strand2 = None
        seen_gene = 0
        seen_strand =False
        for infPiec in info_pieces:
            if infPiec == 'SNP':
                continue
            key = infPiec.split('=')[0]
            value = infPiec.split('=')[1]
            if key == 'GENE':
                if seen_gene == 0:
                    gene1 = value
                    seen_gene = 1
                elif seen_gene == 1:
                    gene2 = value
                    seen_gene = 2
                else:
                    'How did it see a third gene!!!'
                    sys.exit(-1)
            elif key == 'STRAND':
                if seen_strand == 0:
                    strand1 = value
                    seen_strand = 1
                elif seen_strand == 1:
                    strand2 = value
                    seen_strand = 2
                else:
                    'How did it see a third strand!!!'
                    sys.exit(-1)
        if gene1 != gene2:
            print('at input ' + str(line) + ' The two gene values in the INFO section are not the same!') 
        if strand1 != strand2:
            print('at input ' + str(line) + ' The two strand values in the INFO section are not the same!')
    wf.close()
    return

cosmic_vcf_coding = 'CosmicCodingMuts.vcf'
cosmic_vcf_noncoding = 'CosmicNonCodingVariants.vcf'
cosmic_tsv_mutants = 'CosmicMutantExport.tsv'

c = COSMIC_Processor()
c.main(cosmic_vcf_coding, cosmic_vcf_noncoding, cosmic_tsv_mutants)
