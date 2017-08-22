import os
import sys
import vcf
import re
import requests
import urllib2
import tarfile
options_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'WebContent','WEB-INF','Wrappers')
sys.path.insert(0, options_dir)
import options




# DMG 7/2/2015
#     Changed timming again. Takes place in options.trimming_vcf_input()
#     Now the end is trimmed first, EXCEPT the first nucleotide which remains!!!
#     The beginning is then trimmed and position is affected
#     This is important for VCF format


# Now imports options.py so you need to put CRAVAT/WebContent/WEB-INF/Wrappers/options.py in the same folder as this file


top_folder = os.path.join('C:\\','Users','Kyle','annot_liftover','esp')
gz_name = 'ESP6500SI-V2-SSA137.GRCh38-liftover.snps_indels.vcf.tar.gz'

data_filename_prefix = 'ESP6500SI-V2-SSA137.GRCh38-liftover.'
data_filename_suffix = '.snps_indels.vcf'
mysql_filename_prefix = 'info.'
mysql_filename_suffix = '.tsv.38'
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '1234'
db_name = 'annot_liftover'
table_prefix = 'esp6500_'
table_scheme = 'chr varchar(5), position int, '+\
               'refbase varchar(110), altbase varchar(100), mutation_type varchar(4), '+\
               'ea_pop_af float, aa_pop_af float'
table_fields = 'chr, position, refbase, altbase, mutation_type, ea_pop_af, aa_pop_af'
chrs = ['chr1','chr2','chr3','chr4','chr5','chr6','chr7',\
        'chr8','chr9','chr10','chr11','chr12','chr13','chr14',\
        'chr15','chr16','chr17','chr18','chr19','chr20','chr21',\
        'chr22','chrx', 'chry']

websiteName = re.compile(gz_name)      #Title on the webpage indicating what document needs to be downloaded

#This function goes through a web page and identifies the URL that needs to be downloads
#In this case it is the URL for the ESP6500 in VCF format
#The function will go through each line and see if there is the start of a link signified as <a
#After the start of a link is seen the end of a link will be searched for, signified as </a>
#All the links <a>.</a> will be examined to see if they are labeled with the title of the desired webpage
#The URL for this will be stored and used to download the VCF file
def search_links():
    
    #Set the regular expression needed for extracting the URL needed for downloading the ESP6500 VCF file
    start_link = re.compile('<a')       # Start of link to URL
    end_link = re.compile('</a>')       # End of link to URL
    
#    Full text of the webpage containing the title/URL for the ESP6500 VCF format desired
    full_webpage_contents = requests.get('http://evs.gs.washington.edu/EVS/')
    
#     Split the text by new line character. Creating this array is necessary for looping through the information correctly
    file_split_by_new_line_character = full_webpage_contents.text.split("\n")
    
#     This is the variable that will hold the final value of the URL desired
    link_for_download = None


    start_of_link = None
    end_of_link = None
    whole_link = ""
    look_for_start = 1          #This starts as a 1, signifying to look because you immidiately need to start looking for a start
    look_for_end = 0
    end_found = 0
    
    for each_line_on_webpage in file_split_by_new_line_character:
        end_found = 0
        
        #Look for <a in the line.
        if look_for_start == 1:
            contains_start = re.search(start_link, each_line_on_webpage)
            #If <a exists record the position in the line and start looking an end. Stop looking for a start
            if contains_start:
                start_of_link =  contains_start.start()
                look_for_end = 1
                look_for_start = 0
        
#         Look for a </a> in the line
        if look_for_end == 1:
            contains_end = re.search(end_link, each_line_on_webpage)
#             If </a> exists record the position in the line, signify that a full link has been found and stop looking another end and allowing searching for a start to begin
            if contains_end:
                
                end_of_link = contains_end.end()
                look_for_start = 1
                look_for_end = 0
                end_found = 1
        
#         If a full link, <a></a> has been found
        if end_found == 1:
#             The if occurs if the full link was found on the same line (array position)
            if start_of_link and end_of_link:
                whole_link = each_line_on_webpage[start_of_link:end_of_link]
                
#                 If a whole link has been found send it to the function check_link() to see if that link is the one you are looking for
                link_needed = check_link(whole_link)
#             The else occurs if the link spans two or more lines (this is more that one array position)
            else:
                whole_link += each_line_on_webpage[0:end_of_link]
                whole_link = whole_link.replace("\n", "")
#                 If a whole link has been found send it to the function check_link() to see if that link is the one you are looking for
                link_needed = check_link(whole_link)
                
#             If the returned value from check_link() does not equal None and thus is the link you are looking for then record it 
            if link_needed:
                link_for_download = link_needed
                
            whole_link = ""
            
#         If the link you are looking for spans more than one array position then this is needed
        if look_for_start == 0 and look_for_end == 1:
            whole_link += each_line_on_webpage[start_of_link:]
    
        start_of_link = None
        end_of_link = None
        
        
    print link_for_download
    
    return link_for_download


# This function searches all the full links and pulls out the one desired
# The variable websiteName is regular expression holding the name of the website desired
def check_link(link):
    
    link_needed = None
#     This is testing to see if the link contains the website name desired that is specfied globally above
    has_website = re.search(websiteName, link)
#     If the full link contains the website name desired pull out the URL that is actually targetd
    if has_website:
        link_needed = link[link.index('href="')+6:link.index('"',link.index('href="')+6)]
        
    
    return link_needed
    


# This function takes the URL extracted from the website and downloads the corresponding vcf tar gz file for ESP6500
def download_file (gz_link):

    response = urllib2.urlopen(gz_link)
    content = response.read()
    gz_path = os.path.join(top_folder, gz_name)
    gz_file = open(gz_path, 'wb')
    gz_file.write(content)
 
    return gz_path


# This function unpacks the vcf.tar.gz files that you downloaded
def unpack (gz_path):

    tfile = tarfile.open(gz_path)
    vcf_dir =  gz_path[:-7]

    if not os.path.exists(vcf_dir):
        os.makedirs(vcf_dir)

    tfile.extractall(vcf_dir)

    return vcf_dir

def make_input_files (vcf_dir):  #You need to fix this so the file of uncompressed vcf is only opened once
    for chr_ in chrs:
        print chr_
        mysql_filename = os.path.join(vcf_dir, mysql_filename_prefix+chr_+'.tsv')
        mysql_file = open(mysql_filename, 'w')
        
#         When the VCF file is unpacked the 'x' and 'y' files are labeled with a capital 'X' and 'Y'. This needs to be taken into account on the 
#         linux box. The following if and else statement do that
        data_filename = None
        if (chr_ == 'chrx' or chr_== 'chry'):
            the_x_or_y = chr_[3]
            capitalize_the_chr_letter = the_x_or_y.upper()
            chr_with_capital_letter = 'chr' + capitalize_the_chr_letter
            data_filename = os.path.join(vcf_dir, data_filename_prefix + chr_with_capital_letter + data_filename_suffix)
        else:
            data_filename = os.path.join(vcf_dir, data_filename_prefix + chr_ + data_filename_suffix)

        f = open(data_filename)
        count = 0
        for entry in vcf.Reader(f):
            count += 1
            if count % 50000 == 0:
                print ' ', count
            chrom = entry.CHROM
            pos = entry.POS
            ref = entry.REF
            alts = entry.ALT
            
#         You do this so that in the database the 'x' and 'y' will be lower case
            if chrom == 'X' or chrom == 'Y':
                chrom = chrom.lower()
                
                
#         This is done so in the database in the chromosome table the column 'chr' has for example 'chr1'
            chrom = 'chr' + chrom
                
                             
            # Gets allele counts.
            ea_acs = []
            for ea_ac in entry.INFO['EA_AC']:
                ea_acs.append(float(ea_ac))
            ea_ac_total = sum(ea_acs)
            aa_acs = []
            for aa_ac in entry.INFO['AA_AC']:
                aa_acs.append(float(aa_ac))
            aa_ac_total = sum(aa_acs)
             
            for alt_no in xrange(len(alts)):
                # Calculates allele frequencies.
                ea_ac = ea_acs[alt_no] / ea_ac_total
                aa_ac = aa_acs[alt_no] / aa_ac_total
                 
                alt = str(alts[alt_no])
                
#                 Trim the sequences. Front first and End second. SO 'GCTCA' -> 'GTCTCA' becomes '-' -> 'T'
#                 Since what you are reading is in VCF format. Putting '+' as the strand is ok
                new_ref2, new_alt2, new_pos = options.trimming_vcf_input(ref, alt, pos, '+')
                               

                if new_ref2 == '':
                    new_ref2 = '-'
                if new_alt2 == '':
                    new_alt2 = '-'
                new_ref2_len = len(new_ref2)
                new_alt2_len = len(new_alt2)
                if new_ref2 == '-' and new_alt2 != '-': # Insertion
                    if new_alt2_len % 3 == 0:
                        mutation_type = 'iv'
                    else:
                        mutation_type = 'fv'
                elif new_ref2 != '-' and new_alt2 == '-': # Deletion
                    if new_ref2_len % 3 == 0:
                        mutation_type = 'iv'
                    else:
                        mutation_type = 'fv'
                elif new_ref2_len == 1 and new_alt2_len == 1: # SNP
                    mutation_type = 'snp'
                else:
                    mutation_type = 'cs'
                    

                mysql_file.write(chrom+'\t'+str(new_pos)+'\t'+new_ref2+'\t'+new_alt2+'\t'+mutation_type+'\t'+str(ea_ac)+'\t'+str(aa_ac)+'\n')
        f.close()
        mysql_file.close()

def create_mysql_tables (vcf_dir):
    import MySQLdb
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    cwd = os.getcwd()
    for chr_ in chrs:
        mysql_filename = os.path.join(vcf_dir, mysql_filename_prefix + chr_ + mysql_filename_suffix)
        print mysql_filename
        if os.path.exists(mysql_filename):
            table_name = table_prefix + chr_.lower()
            print chr_, mysql_filename, table_name
            stmt = 'drop table if exists '+table_name
            cursor.execute(stmt)
            db.commit()
            stmt = 'create table '+table_name+' ('+table_scheme+') engine=innodb'
            cursor.execute(stmt)
            db.commit()
            stmt = 'load data local infile \'%s\' into table %s (%s)' \
                    %(mysql_filename.replace('\\','/'), table_name, table_fields)
            cursor.execute(stmt)
            stmt = 'create index '+table_name+'_idx on '+table_name+'(position)'
            cursor.execute(stmt)
            db.commit()

def show_maximum_reference_alternate_base_size ():
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

if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'build':
#         print 'Searching for the download URL'
#         gz_link = search_links()
#         print 'Downloading from: %s' %gz_link
#         gz_path = download_file(gz_link)
#         print 'Uncompressing '+top_folder + gz_name
#         gz_path = os.path.join(top_folder, gz_name)
#         vcf_folder = unpack(gz_path)
#         print 'Making MySQL input files'
#         make_input_files(vcf_folder)
#         vcf_folder = os.path.join(top_folder, 'ESP6500SI-V2-SSA137.GRCh38-liftover.snps_indels.vcf')
        vcf_folder = os.path.join('C:\\','Users','Kyle','annot_liftover','esp','ESP6500SI-V2-SSA137.GRCh38-liftover.snps_indels.vcf','38')
        print 'Creating MySQL tables for esp6500'
        create_mysql_tables(vcf_folder)
    elif cmd == 'max':
        print 'Checking maximum reference and alternate base size'
        show_maximum_reference_alternate_base_size()
        
        
        
        
        
        
        
        
        
        
        