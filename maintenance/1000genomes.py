import os
import sys
import re
import requests
import options
import traceback


# Insides the make_mysql_input_files function there is an 'if release == "20130502"'
#     When this is true things are done correctly. Otherwise they are NOT
#     Think about this when you incorporate a new release


# DMG 7/2/2015
#     Changed timming again. Takes place in options.trimming_vcf_input()
#     Now the end is trimmed first, EXCEPT the first nucleotide which remains!!!
#     The beginning is then trimmed and position is affected
#     This is important for VCF format

# Now imports options.py so you need to put CRAVAT/WebContent/WEB-INF/Wrappers/options.py in the same folder as this file
datafile_dir = os.path.join('C:\\','Users','Kyle','db_update','kg')
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '1234'
db_name = 'cravat_annot_dev'
table_prefix = 'thousandgenomes_'
data_filename_prefix = 'info.'
table_scheme = 'chr varchar(5), position int, '+\
               'refbase varchar(100), altbase varchar(100), '+\
               'ac int, an int, af float, variant_type varchar(5)'
table_fields = 'chr, position, refbase, altbase, ac, an, af, variant_type'
chrs = ['chr1','chr2','chr3','chr4','chr5','chr6','chr7',\
        'chr8','chr9','chr10','chr11','chr12','chr13','chr14',\
        'chr15','chr16','chr17','chr18','chr19','chr20','chr21',\
        'chr22','chrx', 'chry']             #Added chrY on 12.18.2014 for the new 1000 genomes release 
ftp_site = 'ftp-trace.ncbi.nih.gov'
ftp_dir = '1000genomes/ftp/release/20130502/'
release = '20130502'
download_filename = 'ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz'

def download_original_data_file ():
    from ftplib import FTP
    ftp=FTP(ftp_site)
    ftp.login()
    ftp.cwd(ftp_dir)
    ftp.retrbinary('RETR '+download_filename, open(os.path.join(datafile_dir,download_filename), 'wb').write)
    ftp.quit()

def uncompress_downloaded_original_data_file ():
    import gzip
    f = gzip.GzipFile(os.path.join(datafile_dir,download_filename))
    thousandgenomes_uncompressed_filename = '.'.join(download_filename.split('.')[:-1])
    wf = open(os.path.join(datafile_dir,thousandgenomes_uncompressed_filename), 'w')
    l = 0
    try:
        for line in f:
            l += 1
            wf.write(line)
    except:
        print 'Line: %s' %l
        print traceback.format_exc()
    finally:
        wf.close()
        f.close()
        exit()

def make_mysql_input_files ():
    
    regExForSV = re.compile('^<.*>?')
    
    original_filename = os.path.join(datafile_dir,download_filename[:-3])
    info_filename_prefix = os.path.join(datafile_dir, 'info.')
    info_files = {}
    
    
    for chr_ in chrs:
        snp_info_filename = info_filename_prefix+chr_+'.tsv'
        snp_info_file = open(snp_info_filename, 'w')
        info_files[chr_] = snp_info_file
    f = open(original_filename)         #Maybe you should put that this is reading. Default is reading but it might be good to put there anyway.
    
    if release == '20130502':
        numberVariantsLookedAt = 0
        startLooking = 0;
        for line in f:
            columnsInLine = line.strip().split('\t')
            
#           This occurs once the line where #CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO occurs has been seen. So the line after it is the first to be looked at 
            if startLooking == 1:
                numberVariantsLookedAt += 1

#                 This is not necessary but I am doing it to check twice. The scope in python just makes me nervous
                chr_ = None
                position = None
                dbsnp = None
                ref = None
                alts_for_spot = None
                
                [chr_, position, dbsnp, ref, alts_for_spot] = columnsInLine[:5]
                
                if chr_ == "X" or chr_ == "Y":
                    chr_ = chr_.lower()
                
                chr_ = 'chr'+chr_
                position = int(position)        # Converting position to an integer so that you can add to it later
                
                
                if numberVariantsLookedAt % 50000 == 0:
                    print 'variant number = ' + str(numberVariantsLookedAt) + '  ' + chr_ + '  pos = ' + str(position)
                
                all_alts = None
                all_alts = alts_for_spot.split(",")
                
                
#               Sometimes there are multiple SNPs or INDELs in one position. This needs to be taken into account
                for numberAltExamining in range(0,len(all_alts)):
                    
                    variant_type = None
                    alt = None
                    alt = all_alts[numberAltExamining]
                
#                 If the alt that you are examining mathces the regEx above then the variant is 'SV'. 
#                 This occurs when an 'alt' is something like <CN2>
                    if (regExForSV.search(alt)):
#                         print alt
                        variant_type = 'SV'
                    else:
                        length_ref = None
                        length_alt = None
                
                        length_ref = len(ref)
                        length_alt = len(alt)
                
#               If the the ref and alt length are equal then its a 'SNP'. if the difference is under 40 there are an INDEL. Over 40 and a SV

                        if length_ref == length_alt:
                            variant_type = 'SNP'
                        elif abs(length_ref - length_alt) < 40:
                            variant_type = 'INDEL'
                        elif  abs(length_ref - length_alt) >= 40:
                            variant_type = 'SV'
                        else:
                            print 'Something Unexpected happened'
                            print 'alt is ' + alt
                            for chr_ in chrs:
                                info_files[chr_].close()
                            f.close()
                            sys.exit(-1)
                
#               Do NOT record a mutation considered a 'SV'. On the other hand for 'SNP' or 'INDEL' do the following
                    if variant_type == 'SNP' or variant_type == 'INDEL':
                        
#                         This is where you are trimming the ref and alt sequence so something like ref: AAT and alt: AAC becomes ref: T and alt C
#                         Whenever something is removed from the beginning you will also adjust the position of the variant
#                         ref and alt are the variable for the reference genotype and alternative genotype
                                                
#                         Trimming the sequences, front first, then end GCTCA -> GTCTCA becomes - -> T
#                         When looking at VCF format you are using the '+' strand
#                         Make sure you are using VCF format!
                        ref_New2, alt_New2, position_New = options.trimming_vcf_input(ref, alt, position, '+')
                                                                        
#          If either sequence gets trimmed off all the way make it equal to '-' signifying an indel
                        
                        if ref_New2 == '':
                            ref_New2 = '-'
                        if alt_New2 == '':
                            alt_New2 = '-'
                            
                        ref_final_len = len(ref_New2)
                        alt_final_len = len(alt_New2)
                        
                        
                        
                        variant_type = None         #This originally equalled 'SNP' or 'INDEL' above but now I am cleaning it. I will also re-assign it
                        
                        if ref_New2=='-' and alt_New2 != '-':       #INSERTION
                            if alt_final_len % 3 == 0:
                                variant_type = 'iv'
                            else:
                                variant_type = 'fv'
                        elif ref_New2 != '-' and alt_New2 == '-':     #DELETION
                            if ref_final_len % 3 == 0:
                                variant_type = 'iv'
                            else:
                                variant_type = 'fv'
                        elif ref_final_len == 1 and alt_final_len == 1:
                            variant_type = 'snp'
                        else:
                            variant_type = 'cs'
                        
                        
#                   Go through the INFO for the line and pick out AF=, AN=, and AC=. There will only be one of each in a line
                        ac = None
                        an = None
                        af = None
#                       Position 7 in the columnsInLine is the info
#                       Want to get the 'AF', 'AN', and 'AC' from the row NOT things like EAS_AF, AMR_AF, AFR_AF, EUR_AF
#                       which was originally a problem. So now 'info[0:3]' is set to == 'AF='. This guarantees only 'AF'
#                       comes before the '=' sign
                        for info in columnsInLine[7].split(';'):
#                             If the info.split(",")[1] is 'AF=' or 'AC=' you have to split that and pic the one that
#                             matches the 'alt' you are looking at. So far there is only 1 'AN' per row so don't split that
                            if info[0:3] == 'AF=':
                                afs_there = info.split('=')[1]
                                af = afs_there.split(",")[numberAltExamining]
                            elif info[0:3] == 'AN=':
                                an = info.split('=')[1]
                            elif info[0:3] == 'AC=':
                                acs_there = info.split('=')[1]
                                ac = acs_there.split(",")[numberAltExamining]
                    
                        data_to_write = [chr_, str(position_New), ref_New2, alt_New2, ac, an, af, variant_type]  #Converting position back to a string so I can write it
                        info_files[chr_].write('\t'.join(data_to_write)+'\n')      
#           The following occurs at every line before and just when you hit the line with      #CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO occurs.    
            else:
                if (columnsInLine[0] == '#CHROM' and columnsInLine[1] == 'POS'):
                    startLooking = 1
#     This else REALLY DOES NOT DO WELL!!
#     DO NOT USE THIS EVER          
    else:
        for line in f:
            snp_flag = line.count('VT=SNP')
            indel_flag = line.count('VT=INDEL')
            sv_flag = line.count('VT=SV')
            if snp_flag > 0 or indel_flag > 0: # or sv_flag > 0:
                if snp_flag > 0:
                    variant_type = 'SNP'
                elif indel_flag > 0:
                    variant_type = 'INDEL'
                elif sv_flag > 0:
                    variant_type = 'SV'
                toks = line.strip().split('\t')
                [chr_, position, dbsnp, ref, alt] = toks[:5]
                chr_ = 'chr'+chr_
                for info in toks[7].split(';'):
                    if info == 'AF':
                        af = info.split('=')[1]
                    elif info == 'AN':
                        an = info.split('=')[1]
                    elif info == 'AC':
                        ac = info.split('=')[1]
                data_to_write = [chr_, position, ref, alt, ac, an, af, variant_type]
                info_files[chr_].write('\t'.join(data_to_write)+'\n')
    f.close()
    for chr_ in chrs:
        info_files[chr_].close()

def create_mysql_tables ():
    import MySQLdb
    db = None
    cursor = None
    try:
        db = MySQLdb.connect(host=mysql_host,\
                             user=mysql_user,\
                             passwd=mysql_password,\
                             db=db_name)
        cursor = db.cursor()
        # create database if not exists thousandgenomes;
        for chr_ in chrs:
            data_filename = os.path.join(datafile_dir,data_filename_prefix+chr_+'.tsv')
            print data_filename
            if os.path.exists(data_filename):
                table_name = table_prefix + chr_
                print chr_, table_name
                stmt = 'drop table if exists '+table_name
                cursor.execute(stmt)
                db.commit()
                stmt = 'create table '+table_name+' ('+table_scheme+') engine=innodb'
                cursor.execute(stmt)
                db.commit()
                stmt = 'load data local infile \'%s\' into table %s (%s)' %(data_filename.replace('\\','/'), table_name, table_fields)
#                 stmt = 'load data local infile '+data_filename+'"'+\
#                        ' into table '+table_name+' ('+table_fields+')'
                cursor.execute(stmt)
                stmt = 'create index '+table_name+'_idx on '+table_name+'(position)'
                cursor.execute(stmt)
                db.commit()
    except:
            print traceback.format_exc()
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            db.close()
        except:
            pass

def show_maximum_reference_alternate_base_size ():
    max_ref_base_size = 0
    max_alt_base_size = 0
    for chr_ in chrs:
        filename = data_filename_prefix+chr_+'.tsv'
        f = open(filename)
        for line in f:
            toks = line.strip().split('\t')
            ref_base_size = len(toks[3])
            alt_base_size = len(toks[4])
            if ref_base_size > max_ref_base_size:
                max_ref_base_size = ref_base_size
            if alt_base_size > max_alt_base_size:
                max_alt_base_size = alt_base_size
    print 'max_ref_base_size =',max_ref_base_size,', max_alt_base_size =',max_alt_base_size

if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'build':
        print 'Downloading '+download_filename
        download_original_data_file()
        print 'Uncompressing '+download_filename
        uncompress_downloaded_original_data_file()
        print 'Making MySQL input files'
        make_mysql_input_files()
        print 'Making the MySQL tables'
        create_mysql_tables()
    elif cmd == 'max':
        print 'Checking maximum reference and alternate base size'
        show_maximum_reference_alternate_base_size()