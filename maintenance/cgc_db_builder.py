import sys
import os
import csv
import traceback
import paramiko
import MySQLdb

# Functions go here

def download_sftp(socket, user, pswd, path_sftp, path_local):
    t = paramiko.Transport(socket)
    t.connect(username=user, password=pswd)
    sftp = paramiko.SFTPClient.from_transport(t)
    sftp.get(path_sftp, path_csv)

# Make a tsv formatted file for each chromosome that holds the data that will be read into the SQL table
def make_input_file(path_in, path_out):
    line_num = 0
    try:
        with open(path_in, 'rb') as file_in:
            with open(path_out, 'wb') as file_out:
                csv_in = csv.DictReader(file_in)
                csv_out = csv.writer(file_out, delimiter = '\t')
                
                for line in csv_in:
                    line_num +=1
                    row = []
                    row.append(line['Gene Symbol'])
                    row.append(line['Mutation Types'])
                    row.append(line['Role in Cancer'].replace('oncogene','Oncogene'))
                    if line['Somatic'] or line['Germline']:
                        if line['Somatic'] and line['Germline']:
                            row.append('somatic/germline')
                        elif line['Somatic']:
                            row.append('somatic')
                        else:
                            row.append('germline')
                    else:
                        row.append('')
                    row.append(line['Tumour Types(Somatic)'])
                    row.append(line['Tumour Types(Germline)'])
                    # Write the row into the tsv file
                    csv_out.writerow(row)

    except:
        print 'Error on line %s' %line_num
        print traceback.format_exc()
        
# Read the tsv formatted chromosome input files into their own tables 
def write_db(db, c, input_src, fields, schema, indx):
    table_name = 'cgc'

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
            %( input_src.replace('\\','/'), table_name, fields )
    c.execute(stmt)
    
    # Create an index on the position column
    stmt = 'create index %s_indx on %s (%s);' %( table_name, table_name, indx)
    c.execute(stmt)

#############################################################################
if __name__ == '__main__':
    if sys.argv[1] == 'build':
        curdir = os.path.dirname(os.path.abspath(__file__))
        path_main = os.path.join(curdir, 'cgc')
        if not(os.path.exists(path_main)): os.mkdir(path_main)
     
        hostname = 'sftp-cancer.sanger.ac.uk'
        port = 22
        username = 'rkim37@jhu.edu'
        password = '11111111'
        path_sftp = 'files/grch37/cosmic/v77/cancer_gene_census.csv'
        filename = path_sftp.split('/')[-1]
        
        mysql_host = '192.168.99.100'
        mysql_port = 3307
        mysql_user = 'root'
        mysql_passwd = '1'
        db_name = 'cravat_annot'
        table_schema = """hugo varchar(25),  mut_types varchar(25), role varchar(25), inheritance varchar(16),  
                          tumor_types_somatic varchar(200), tumor_types_germline varchar(200)"""
        table_fields = 'hugo, mut_types, role, inheritance, tumor_types_somatic, tumor_types_germline'

        path_csv = os.path.join(path_main, filename)
        path_sql_file = os.path.join(path_main, 'gene_level.tsv')
        
        print 'Downloading from: %s' %hostname
        download_sftp( (hostname, port), username, password, path_sftp, path_csv)
        print 'Downloaded to: %s' %path_csv
        
        print 'Making SQL input file'
        make_input_file(path_csv, path_sql_file)
        print 'SQL file created: %s' %path_sql_file
        
        print 'Writing to SQL Database'
        db = MySQLdb.connect(host=mysql_host,\
                             port=mysql_port,\
                             user=mysql_user,\
                             passwd=mysql_passwd,\
                             db=db_name)
        print 'SQL connection successful'
        c = db.cursor()
        try:
#             write_db(db, c, sql_files_dir, table_fields, table_schema, 'position')
            write_db(db, c, path_sql_file, table_fields, table_schema, 'hugo')
        except:
            print traceback.format_exc()
        finally:
            c.close()
            db.close()
            print 'Database creation complete'