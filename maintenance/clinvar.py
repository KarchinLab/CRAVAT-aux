import sys
import os
import traceback
from ftplib import FTP
from gzip import GzipFile
import csv
import re
import MySQLdb


# Download a file from an ftp site
def download_ftp(ftp_url, ftp_path, ftp_filename, download_dir):
    ftp=FTP(ftp_url)
    ftp.login()
    ftp.cwd(ftp_path)
    download_path = os.path.join(download_dir, ftp_filename)
    ftp.retrbinary( 'RETR %s' %ftp_filename, open(download_path, 'wb').write )
    return download_path


# Uncompress a gzip compressed file
def uncompress(path_file):
    path_uncomp = path_file.replace('.gz','')
    with GzipFile(path_file) as file_gz:
        with open(path_uncomp,'wb') as file_uncomp:
            file_uncomp.write(file_gz.read())
    return path_uncomp
  
    
# Remove all NULL bytes from a file    
def remove_null_bytes(path_main):
    path_temp = os.path.join( os.path.dirname(path_main), 'temp')
    # Read data from original file, remove NULL bytes, the write to a temp file
    with open(path_main,'r') as f:
        with open(path_temp,'w') as f_temp:
            for line in f:
                f_temp.write(line.replace('\0',''))
    # Delete and the temp file will be renamed
    os.remove(path_main)
    os.rename(path_temp, path_main)


# Check if a line should be included in the database.
def include_line(line):
    # !!! line is a dict, not a str or list
    if not(line['Assembly'] == 'GRCh38'):
        return False
    elif line['Chromosome'] == 'MT':
        return False
    elif 'Pathogenic' not in line['ClinicalSignificance']:
        return False
    else:
        return True


# Make the reference dicts for looking up diseases
def make_ref_dict(path_tsv):
    d = {'omim':{},'medgen':{}}
    with open(path_tsv) as f:
        f.readline()
        for line in f:
            line_split = line.split('\t')
            d['omim'][line_split[4]] = line_split[0]
            d['medgen'][line_split[2]] = line_split[0]
    return d


def make_gene_input_files(path_in, path_out):
    with open(path_in, 'rb') as file_in:
        with open(path_out, 'wb') as file_out:
            tsv_in = csv.reader(file_in, delimiter='\t')
            tsv_in.next()
            tsv_out = csv.writer(file_out, delimiter='\t')
            
            for line in tsv_in:
                row = line[:4]
                row.append( '%s:%s' %(line[4], line[5]) )
                row.append( line[6] )
                tsv_out.writerow(row)
        
    return path_out


# Make a tsv formatted file for each chromosome that holds the data that will be read into the SQL table
def make_input_files(path_tsv, keep_cols, ref_dicts, out_dir):
    if not(os.path.exists(out_dir)):
        os.mkdir(out_dir)
    
    # These will be used to search for disease accession numbers
    re_omim = re.compile('OMIM:(\d+)')
    re_medgen = re.compile('MedGen:(C\d+|CN\d+)')
    
    # Make chr#.tsv files and generate the tsv objects
    poss_chroms = ['chr%d' %cnum for cnum in range(1,23)]
    poss_chroms.append('chrX')
    poss_chroms.append('chrY')
    chroms_file = {}
    chroms_csv = {}
    for c in poss_chroms:
        chroms_file[c] = open( os.path.join(out_dir, '%s.tsv' %c), 'wb' )
        chroms_file[c].truncate()
        chroms_csv[c] = csv.writer(chroms_file[c], delimiter='\t')
    
    # Write lines to correct tsv file    
    with open(path_tsv, 'r') as f:
        file_tsv = csv.DictReader(f, delimiter='\t')
        line_num = 0
        for line in file_tsv:
            line_num +=1
            
            # Check that line meet criteria to be included  
            if include_line(line):
                # Chrom must be properly formatted (chr1, chr2, chrx, chry)
                chrom = 'chr%s' %(line['Chromosome'].upper())
                
                # Row is a list of the values that will be written, in order, to a single row of output
                row = [chrom]
                # Append the defined values to row
                for col in keep_cols:
                    row.append(line[col])
                # Use correct cravat pos for insertions
                if row[2] == '-':
                    row[1] = str(int(row[1])+1)
                    
                # Use the ref_dicts to look up the diseases specified in PhenotypeIDs
                pheno_ids = line['PhenotypeIDS']   
                try: 
                    disease_list = []
                    omim_nums = re.findall(re_omim, pheno_ids)
                    medgen_nums = re.findall(re_medgen, pheno_ids)
                    if not(omim_nums or medgen_nums):
                        disease_list.append('-')                            
                    else:
                        medgen_diseases = []
                        omim_diseases = []
                        for num in medgen_nums:
                            try: medgen_diseases.append(ref_dicts['medgen'][num])
                            except: pass
                        for num in omim_nums:
                            try: omim_diseases.append(ref_dicts['omim'][num])
                            except: pass
                        disease_list = medgen_diseases
                        disease_list.extend(d for d in omim_diseases if d not in medgen_diseases)
                    if not(disease_list):
                        disease_list.append('-')
                    # Add diseases to row, use a ; as a seperator
                    row.append(';'.join(disease_list))
                except:
                    print num
                    if disease_list:
                        row.append(';'.join(disease_list))
                    else:
                        row.append('-')
                
                # Write the row into the tsv file
                chroms_csv[chrom].writerow(row)
   
    return out_dir


# Read the tsv formatted chromosome input files into their own tables 
def write_db(db, c, input_src, fields, schema, indx):
    if os.path.isdir(input_src):
        list_files = os.listdir(input_src)
        input_dir = input_src
    elif type(input_src) is str:
        list_files = [os.path.basename(input_src)]
        input_dir = os.path.dirname(input_src)
    else:
        exit()
        
    for filename_chrom in list_files:
        path_input = os.path.join(input_dir, filename_chrom)
        table_name = 'clinvar_%s' %filename_chrom.split('.')[0]
        
        print '\t%s' %table_name
        
        # Drop the original table if it exists.
        # !!! If table does not already exist, this will throw a warning
        stmt = 'drop table if exists %s;' %table_name
        c.execute(stmt)
        
        # Create the table according to the schema
        stmt = 'create table %s (%s) engine=innodb;' %( table_name, schema)
        c.execute(stmt)
        
        # Load the tsv into the table, using the fields defined in the parameters
        stmt = """load data local infile '%s' 
                  into table %s 
                  fields terminated by '\\t' 
                  optionally enclosed by '"'
                  lines terminated by '\r\n'
                  (%s);
                  """ \
                %( path_input.replace('\\','/'), table_name, fields )
        c.execute(stmt)
        
        # Create an index on the position column
        stmt = 'create index %s_indx on %s (%s);' %( table_name, table_name, indx)
        c.execute(stmt)
    

########################################################################################################    
if __name__ == '__main__':
    if sys.argv[1] == 'build':
        path_main = os.path.join(os.getcwd(), 'clinvar')
        if not(os.path.exists(path_main)):
            os.mkdir(path_main)
            
        ftp_url = 'ftp.ncbi.nlm.nih.gov'
        ftp_path = '/pub/clinvar/tab_delimited/'
        ftp_filename = 'variant_summary.txt.gz'
        ftp_path_ref = '/pub/clinvar'
        ftp_filename_ref = 'disease_names'
        ftp_filename_gene = 'gene_condition_source_id'
        
        keep_cols = ['Start','ReferenceAllele','AlternateAllele','ClinicalSignificance','PhenotypeIDS']
        
        mysql_host = 'localhost'
        mysql_port = 3306
        mysql_user = 'root'
        mysql_passwd = '1234'
        db_name = 'annot_liftover'
        table_fields = 'chr, position, refbase, altbase, clin_sig, disease_refs, diseases'
        table_schema = """chr varchar(31), position int, refbase varchar(1000), altbase varchar(1000),
                        clin_sig varchar(100), disease_refs varchar(4000), diseases varchar(1000)"""
        table_gene_fields = 'ncbi_geneid, hugo, medgen, diseases, source, omim'
        table_gene_schema = """ncbi_geneid varchar(10), hugo varchar(15), medgen varchar(8), 
                               diseases varchar(1000), source varchar(200), omim varchar(45)"""
        
        path_gz = os.path.join(path_main, ftp_filename)
        path_uncomp = os.path.join(path_main, ftp_filename.replace('.gz',''))
        path_ref = os.path.join(path_main, ftp_filename_ref)
        sql_files_dir = os.path.join(path_main, 'sql_input_files')
        sql_file_gene = os.path.join(path_main, 'gene_level_diseases.tsv')
        path_gene = os.path.join(path_main, 'gene_condition_source_id')
        print path_ref
         
        print 'Downloading %s' %ftp_filename
        path_gz = download_ftp(ftp_url, ftp_path, ftp_filename, path_main)
        print 'Download complete: %s' %os.path.basename(path_gz)
                 
        print 'Uncompressing %s' %os.path.basename(path_gz)
        path_uncomp = uncompress(path_gz)
        remove_null_bytes(path_uncomp)
        print 'Uncompress complete: %s' %os.path.basename(path_uncomp)
  
        print 'Downloading reference table'
        path_ref = download_ftp(ftp_url, ftp_path_ref, ftp_filename_ref, path_main)
        print 'Download complete: %s' %os.path.basename(path_ref)
          
        print 'Downloading gene table'
        path_gene = download_ftp(ftp_url, ftp_path_ref, ftp_filename_gene, path_main)
        print path_gene
        print 'Download complete: %s' %os.path.basename(path_ref)
  
        print 'Making Gene level input file'
        sql_file_gene = make_gene_input_files(path_gene, os.path.join(path_main, 'gene_level_diseases.tsv'))
         
              
        print 'Making OMIM and MedGen reference dictionaries'
        ref_dicts = make_ref_dict(path_ref)
          
        print 'Making SQL input files'
        sql_files_dir = make_input_files(path_uncomp, keep_cols, ref_dicts, sql_files_dir)
         
        print 'Writing to SQL Database'
        db = MySQLdb.connect(host=mysql_host,\
                             port=mysql_port,\
                             user=mysql_user,\
                             passwd=mysql_passwd,\
                             db=db_name)
        c = db.cursor()
        try:
            write_db(db, c, sql_files_dir, table_fields, table_schema, 'position')
            write_db(db, c, sql_file_gene, table_gene_fields, table_gene_schema, 'hugo')
        except:
            print traceback.format_exc()
        finally:
            c.close()
            db.close()
    