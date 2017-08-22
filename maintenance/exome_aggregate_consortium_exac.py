import sys
import os
import vcf
import MySQLdb
options_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'WebContent','WEB-INF','Wrappers')
sys.path.insert(0, options_dir)
import options

# Now imports options.py so you need to put CRAVAT/WebContent/WEB-INF/Wrappers/options.py in the same folder as this file


# DMG 7/2/2015
#     Changed timming again. Takes place in options.trimming_vcf_input()
#     Now the end is trimmed first, EXCEPT the first nucleotide which remains!!!
#     The beginning is then trimmed and position is affected
#     This is important for VCF format

# DMG 06/29/2015
#     Change the trimming of sequences so that the start of a sequence is examined before the end of a sequence
#     Example:  'G' and 'GAG'  -> '-' and 'AG'

#     From ExAC website  'ftp://ftp.broadinstitute.org/pub/ExAC_release/current/README.known_issues'
# MUST NOTE THESE THINGS!!
#     Especially when looking at AC_Het. Even though this program does not look at AC_Het
    
#     Occurred on Jan. 22, 2015
#     From ExAC website  'ftp://ftp.broadinstitute.org/pub/ExAC_release/current/README.known_issues'
# Fixed issues in 0.3:
# - chrX and chrY now have Hemizygous counts (AC_Hemi) and population specific Hemizygous counts. The AC_Adj has
# also been adjusted accordingly. See README.population_annotations
# 
# - An additional round of QC was performed on a sample level, including removing samples with outlier chrX heterozygosity
# and chrY normalized coverage.
# 
# - Hard filter applied to remove sites with Inbreeding Coefficient < -0.2 that was VQSR failed to remove.
# 
# Fixed issues in 0.2:
# 
# - The VQSR 99.6% SNP Sensitivity is too conservative and filters ~17% of singletons. 
# Our analysis of singleton TiTv, Doubleton transmission in Trios, validated de-novo mutations and comparison
# against PCR-Free WGS has shown filtering ~10% of singletons is a better trade off. This corresponds to VQSLOD > -2.632.
# 
# - An additional round of QC was performed on a sample and variant level.
# 
# - Overlapping SNPs and Indels are represented on the same line. This has two major implications for analysis
# i) SNPs that overlap with Indels can be of string length > 1.
# ii) When a subset of samples are extracted from a call set, the representation of REF and ALT alleles have been adjusted
# to their minimal representation for bi-allelic sites if a variant no longer exist in the new call set.
# 
# - AC_Hom annotation has been corrected and is allele specific in the following order (eg. 3 alt alleles) 1/1, 2/2, 3/3
# 
# - AC_Het is allele specific, showing all possible pairs in the follwing order (eg. 3 alt alleles) 0/1,0/2,0/3,1/2,1/3,2/3
# 
# - Hard filter applied to remove sites with Inbreeding Coefficient < -0.8 that was VQSR failed to remove.
# 
# - On producing a subset there are sites with AC_Adj=0. This occurs when there are high quality calls in the larger call set but after
# subsetting the data only lower quality scores remain. The hard filter AC_Adj0_Filter was added to indicat these sites.
# 
# - chrY variants added to the call set and also corresponding coverage summary included.
# 
# Known issues:
# 
# - The variant effect predictor (VEP) tool used to annotate the sites only call set and contains a comprehensive list
# of transcript annotations. In future releases we may switch to Gencode basic transcript annotations to reduce
# this number.
# 
# - AC_Adj=0 filter is not allele specific but correction has been made on ExAC Browser.















#This program downloads the Exome Aggregation Consortium (ExAC) datafile from the ExAC ftp site and then pulls
#the alternative allele and their frequencies from the file and stores them in a database

#    At this time only the non-adjusted collective alternative allele frequency is extracted
#    The ExAC file provides alternative allele frequency data specific to ethnicity however 
#    this program currently does not extract that data



def main(command):
    
#     Variables that are going to be needed
    
    
    #Information for opening the database
    mysql_host = 'localhost'
    mysql_user = 'root'
    mysql_password = '1234'
    db_name = 'annot_liftover'
    
    #Used for naming the database table and the MySQL input files
    table_prefix = 'ExAC_'
    data_filename_prefix = 'info.'
    data_filename_suffix = '.tsv'
    
    
    
    
    #Start construsting the schema for the database tables
    table_schema = 'chr varchar(5), position int, '+\
                'refbase varchar(1000), altbase varchar(1000), mutation_type varchar(4), '+\
                'af_adj_total float'
    table_fields = 'chr, position, refbase, altbase, mutation_type, af_adj_total'
    
    ethnicities_array = ['AFR', 'AMR', 'EAS', 'FIN', 'NFE', 'OTH', 'SAS']
    
    #Finish constructing the table schema to include separate ethnicities. Also put the separate entities in the table_fields variable
    for eth_mktbl in ethnicities_array:
        eth_mktbl = eth_mktbl.lower()
        table_schema += ', af_'+eth_mktbl + ' float'
        table_fields += ', af_'+eth_mktbl
    
    
    #All the chromosomes that will be made in the database. 
    chrs = ['chr1','chr2','chr3','chr4','chr5','chr6','chr7',\
        'chr8','chr9','chr10','chr11','chr12','chr13','chr14',\
        'chr15','chr16','chr17','chr18','chr19','chr20','chr21',\
        'chr22','chrx', 'chry'] 
    
 
    
#     Download the ExAC data file and create database tables with the alternative allele frequency for each chromosome
    if command == 'build':
# Variable needed when downloading and building the ExAC table
        datafile_dir = os.path.join('C:\\','Users','Kyle','annot_liftover', 'exac')     #Place the VCF and MySQL input files are created/stored
        
#    Site holding the ExAC file, the location in the site, the release number, and the name of the file being downloaded
        ftp_site = 'ftp.broadinstitute.org'      
        ftp_dir = '/pub/ExAC_release/release1/'        
        

             
        
        
        try:
            print 'Retrieving downloadable dataset'
#             download_filename = download_ExAC(ftp_site, ftp_dir, datafile_dir)
            
            print 'uncompresses the retrieved dataset'
#             uncomp_filename = uncompress_ExAC(datafile_dir, download_filename)
            uncomp_filename = 'ExAC.r1.sites.vep.vcf'
            print 'making the MySQL input files'
#             make_mysql_ExAC_inputfiles(datafile_dir, uncomp_filename, chrs, data_filename_prefix, data_filename_suffix, ethnicities_array)
#     Connect to the database
            db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
            cursor = db.cursor()
 
            print 'loading the MySQL input files into the database'
            create_ExAC_mysql_tables(db, cursor, table_prefix, table_schema, table_fields, chrs, datafile_dir, data_filename_prefix, data_filename_suffix)
  
            db.commit()     #Not committing until the very end. 
  
            cursor.close()
            db.close()
            print 'Completed loading the ExAC data into the database'
        except MySQLdb.Error, databaseProblemOccurredInMain:
            sys.stderr.write('Error occurred in main in the database: \t\n'+ str(databaseProblemOccurredInMain) + '\n')
            try:
                cursor.close()
                db.close()
            except:
                anotherPlaceHolder = 5
            sys.exit(-1)
        except Exception, e:
            sys.stderr.write('Error occurred in main:\n\t ' + str(e) + '\n')
            try:
                cursor.close()
                db.close()
            except NameError:
                standAlonePlaceHold = 5
            sys.exit(-1)
                
                
    elif command == 'max':
        show_maximum_reference_alternate_base_size_ExAC(chrs, data_filename_prefix, data_filename_suffix)

    return


#Download the most current .vcf.gz file from the ExAC FTP website
def download_ExAC(ftp_site, ftp_dir, datafile_dir):
    
    download_filename = None
    
    try:
    
        from ftplib import FTP
        ftp=FTP(ftp_site)
        ftp.login()
        ftp.cwd(ftp_dir)
    
    #     Retrieve the names of the files in the ftp and pick out the one desired
        file_names = ftp.nlst()
        for name in file_names:
            if name[-7:] == ".vcf.gz":
                download_filename = name
                break
        cur_files = os.listdir(datafile_dir)
        if download_filename not in cur_files:
            ftp.retrbinary('RETR '+download_filename, open(os.path.join(datafile_dir, download_filename), 'wb').write)
            ftp.quit()
        else:
            print 'File %s already present in %s' %(download_filename, datafile_dir)
        return download_filename
    except Exception, prob:
        sys.stderr.write('An error occurred in download_ExAC: \n\t' + str(prob) + '\n')
        sys.exit(-1)





#Uncompress the downloads .vcf.gz file to a .vcf file
def uncompress_ExAC (datafile_dir, download_filename):
    try:
        import gzip
        f = gzip.GzipFile(os.path.join(datafile_dir, download_filename))
        broad63K_uncompressed_filename = '.'.join(download_filename.split('.')[:-1])
        wf = open(os.path.join(datafile_dir,broad63K_uncompressed_filename), 'w')
        for line in f:
            wf.write(line)
    #     print 'gone through whole file'
        wf.close()
        f.close()
        
        return broad63K_uncompressed_filename
    except Exception, prob:
        sys.stderr.write('An error occurred during uncompress_ExAC: \n\t' + str(prob) + '\n')
        sys.exit(-1)

#This function constructs the input files for the MySQL database
#Each chromosome will get a separate table so each chromosome has a separate file, and every line in the file is a row in the MySQL table
def make_mysql_ExAC_inputfiles(datafile_dir, uncomp_filename, chrs, data_filename_prefix, data_fiename_suffix, ethnicities_array):
    
#     Dictionary to hold all the mysql input files for each chromosome.
#     They are all open at the same time so we only have to read through the VCF file once
    info_file_for_each_chr = {}
    
#     make/open the files for each chromosome
    for chromosome in chrs:
        filename_each_chr = data_filename_prefix + chromosome + data_fiename_suffix
        file_each_chr = open(os.path.join(datafile_dir, filename_each_chr), 'w')
        info_file_for_each_chr[chromosome] = file_each_chr
        
#       Open the VCF file that has all the alternative allele frequencies  
    broad63K_file = open(os.path.join(datafile_dir,uncomp_filename), 'r')
    
#     Keep track of the number of lines the vcf.Reader goes through
    count = 0
    
#     Use VCF reader to get the information for each variant in the file
    for vcf_entry in vcf.Reader(broad63K_file):
        
        count += 1
        
        if count % 50000 == 0:
            print '\t ', count        
 
#         Get the chrom, position, ref sequence, alt sequence
#         The VCF reader creates a class for each variant that contains the chrom, position, ref seq, and alt seq
        chrom = vcf_entry.CHROM
#         If chrom X or chrom Y make the X or Y lower case
        if chrom == "X" or chrom == "Y":
            chrom = chrom.lower()
        pos = vcf_entry.POS
        ref = vcf_entry.REF
        alts = vcf_entry.ALT        # This is an array of the alternative sequences at a position
    
#         Get adjusted allele count totals. This will be an array in the same order as the array of alternative sequences
#         The VCF reader class has a value 'INFO' that contains dictionary with the key labeled as 'AC_Adj' that contains an array for the total allele counts for each alternative allele,
#         this is when the ethnicities are NOT split up. This is in the same order as the array for alternative alleles
        alt_adj_allele_counts = vcf_entry.INFO["AC_Adj"]         
        
#         Get the adjusted chromosome count total for a position. There is only one adjusted chromosome count for each position in the VCF file. It is labeled as 'AN_Adj'
        chom_adj_total_count = vcf_entry.INFO["AN_Adj"]

#         The class 'INFO' contains a dictionary of the allele counts for each ethnicity labeled as 'AC_ETHNICITY'. Each allele count in the AC_ETHNICITY is in the same order as the array for alternative alleles.
#         There are 7 different ethnicities in ExAC

        dict_ac_ethnicity_alt_ac = {}
        dict_an_ethnicity_alt_an = {}
        
        for eth_ac_an in ethnicities_array:
            dict_ac_ethnicity_alt_ac['AC_'+eth_ac_an] = vcf_entry.INFO['AC_'+eth_ac_an]
            dict_an_ethnicity_alt_an['AN_'+eth_ac_an] = vcf_entry.INFO['AN_'+eth_ac_an]
 
#         Go through the array of alternative sequences based upon the number of the alternative sequence in the array
#         This works because the alternative sequence, and the respective alternative fequencies are in the same order
        for alt_no in xrange(len(alts)):
            
#             Within the array of alternate sequence pick out the one you are examining using the alt_no. Same with the alternative allele frequency for 
#             that specific alternative sequence
            specific_alt = str(alts[alt_no])        #Make this a string because you are pulling it from a list and it doesn't happen naturally
            
            #This is calculating the total alt frequency for that specific alt. This takes AC_Adj for that specific alt and divides it by the chromosome count for that position.
            # So it will be AF_Adj_total[specific alt] = (AC_Adj[specific alt]/AN_Adj)
            specific_alt_adj_allele_count = alt_adj_allele_counts[alt_no]      
            
            specific_alt_adj_total_frequency = None
            if chom_adj_total_count == 0:
                specific_alt_adj_total_frequency = 0
            else:
                specific_alt_adj_total_frequency = float(specific_alt_adj_allele_count)/float(chom_adj_total_count) 
            
            if specific_alt_adj_total_frequency == 0.0:
                specific_alt_adj_total_frequency = 0            
            
            
            
            
#             Calculate the specific alt allele frequency for each ethnicity.
#             Calculated as AC_Ethnicity/AN_Ethnicity.


#             This calculated the alt allele frequency based on the ethnicity VERY WRONG!!! Best we have for now though
            dict_af_ethnicity_alt_af = {}
            for eth_af in ethnicities_array:
                dict_af_ethnicity_alt_af['AF_'+eth_af] = ac_divided_by_an(dict_ac_ethnicity_alt_ac['AC_'+eth_af][alt_no], dict_an_ethnicity_alt_an['AN_'+eth_af])
            
#             Trimming the sequence. Trim the start first and then end  SO  GCTCA -> GTCTCA becomes - -> T.
#             Can use "+" here to represent the strand type because using VCF formatted file
            new_ref2, new_alt2, new_pos = options.trimming_vcf_input(ref, specific_alt, pos, "+")

            
#             If the beginning of either sequence gets trimmed off all the way make it equal to '-' signifying an indel
            if new_ref2 == '':
                new_ref2 = '-'
            if new_alt2 == '':
                new_alt2 = '-'
                
            new_ref2_len = len(new_ref2)
            new_alt2_len = len(new_alt2)
            
            mutation_type = None
            
            if new_ref2 == '-' and new_alt2 != '-':  #Insertion
                if new_alt2_len % 3 == 0:
                    mutation_type = 'iv'
                else:
                    mutation_type = 'fv'
            elif new_ref2 != '-' and new_alt2 == '-': #Deletion
                if new_ref2_len % 3 == 0:
                    mutation_type = 'iv'
                else:
                    mutation_type = 'fv'
            elif new_ref2_len == 1 and new_alt2_len == 1:
                mutation_type = 'snp'
            else:
                mutation_type = 'cs'
            
            
            dictionary_chr_key = None
            dictionary_chr_key = 'chr' + chrom
            
            mysql_file_writing = None
            
            mysql_file_writing = info_file_for_each_chr[dictionary_chr_key]      #Find which mysql file you are writing to, based upon the chromosome for that alternative allele
            
            line_holding_values = None
            line_holding_values = dictionary_chr_key+'\t'+str(new_pos)+'\t'+new_ref2+'\t'+new_alt2+'\t'+mutation_type+'\t'+str(specific_alt_adj_total_frequency)
            

#             Add the allele frequencies for that specific allele to the output line
            for eth_fill in ethnicities_array:
                line_holding_values += '\t' + str(dict_af_ethnicity_alt_af['AF_'+eth_fill])
            line_holding_values += '\n'
            
#         You are putting the 'dictionary_chr_key in here because in the database you want things to be labeled as chr1 NOT just 1 for example'
            mysql_file_writing.write(line_holding_values)


    broad63K_file.close()
    for each_chr in chrs:
        info_file_for_each_chr[each_chr].close()    
        
    
    return


#This function calculated allele frequency by dividing the allele count (AC) by the total number chromosomes with that gene (AN)
#Used to calculate the allele frequency for each ethnicity
def ac_divided_by_an(allele_count, allele_total):
    allele_frequency = None
    
    if allele_total == 0:
        allele_frequency = 0
    else:
        allele_frequency = float(allele_count)/float(allele_total)
        
    if allele_frequency == 0.0:
        allele_frequency = 0
    
    return allele_frequency




#This funciton deletes a MySQL ExAC chromosome table if it already exists, creates a new one and then fills it with the MySQL input file created in the 
#function 'make_mysql_ExAC_inputfiles'
def create_ExAC_mysql_tables (db, cursor, table_prefix, table_scheme, table_fields, chrs, datafile_dir, data_filename_prefix, data_filename_suffix):

#     cwd = os.getcwd()
    
    try:
        for chr_ in chrs:
            mysql_filename = os.path.join(datafile_dir, data_filename_prefix + chr_ + data_filename_suffix + '.38')
            print mysql_filename
            if os.path.exists(mysql_filename):
                table_name = table_prefix + chr_.lower()
                print '\t', chr_, mysql_filename, table_name
                stmt = 'drop table if exists '+table_name
                cursor.execute(stmt)
#                 db.commit()
                stmt = 'create table '+table_name+' ('+table_scheme+') engine=innodb'
                cursor.execute(stmt)
#                 db.commit()
                stmt = 'load data local infile \'%s\' into table %s (%s)' \
                        %(mysql_filename.replace('\\','/'), table_name, table_fields)
                cursor.execute(stmt)
                stmt = 'create index '+table_name+'_idx on '+table_name+'(position)'
                cursor.execute(stmt)
#                 db.commit()
            else:
                sys.stderr.write('The mysql_filename '+mysql_filename+' does not exist and so that chromosome was skipped.\n\n')

                
    except MySQLdb.Error, databaseProblemOccurred:
        sys.stderr.write("The following problem occurred in create_ExAC_mysql_tables with the database:" + "\n\n" + "%s" %databaseProblemOccurred + "\n\n\n")
        
        cursor.close()
        db.close()
        sys.exit(-1)
    except Exception, fOccurred:
        cursor.close()
        db.close()
        sys.stderr.write("The following problem has occured in create_ExAC_mysql_tables %s" %fOccurred)
        sys.exit(-1)  
                
    return

# This finds the longest variations for a certain chromosome
def show_maximum_reference_alternate_base_size_ExAC (chrs, mysql_filename_prefix, mysql_filename_suffix):
    max_ref_base_size = 0
    max_alt_base_size = 0
    for chr_ in chrs:
        filename = mysql_filename_prefix + chr_ + mysql_filename_suffix
        f = open(filename)
        for line in f:
            toks = line.strip().split('\t')
            ref_base_size = len(toks[2])
            alt_base_size = len(toks[3])
            if ref_base_size > max_ref_base_size:
                max_ref_base_size = ref_base_size
            if alt_base_size > max_alt_base_size:
                max_alt_base_size = alt_base_size
    print 'max_ref_base_size =',max_ref_base_size,', max_alt_base_size =',max_alt_base_size
    
    


if __name__ == "__main__":
    command = sys.argv[1]
    
    main(command)
    
    
    
    
    
    
    