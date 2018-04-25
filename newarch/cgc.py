import sys
import os
import csv
import traceback
import paramiko
import sqlite3

def download_sftp (socket, user, pswd, path_sftp, path_local):
    t = paramiko.Transport(socket)
    t.connect(username=user, password=pswd)
    sftp = paramiko.SFTPClient.from_transport(t)
    sftp.get(path_sftp, path_csv)

def make_input_file (path_in, path_out):
    line_num = 0
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
        
def write_db(db, input_src):
    table = 'cgc'
    
    stmt = 'drop table if exists ' + table
    c.execute(stmt)
    
    stmt = 'create table ' + table + ' (hugo text, mut_types text, ' +\
        'role text, inheritance text, tumor_types_somatic text, ' +\
        'tumor_types_germline text)'
    c.execute(stmt)
    
    sql = 'create index cgc_idx0 on cgc (hugo)'
    c.execute(sql)
    
    f = open(path_sql_file)
    for line in f:
        [v1, v2, v3, v4, v5, v6] = line.strip('\r\n').split('\t')
        sql = 'insert into ' + table + ' values ("%s", "%s", "%s", "%s", "%s", "%s")' % (v1, v2, v3, v4, v5, v6)
        c.execute(sql)
    db.commit()

hostname = 'sftp-cancer.sanger.ac.uk'
port = 22
username = 'rkim37@jhu.edu'
password = 'G+3nX2\'('
path_sftp = 'files/grch38/cosmic/v84/cancer_gene_census.csv'
filename = path_sftp.split('/')[-1]

path_csv = os.path.join(filename)
path_sql_file = os.path.join('cgc.tsv')

print('Downloading from: %s' %hostname)
#download_sftp( (hostname, port), username, password, path_sftp, path_csv)
print('Downloaded to: %s' %path_csv)

print('Making SQL input file')
make_input_file(path_csv, path_sql_file)
print('SQL file created: %s' %path_sql_file)

print('Writing to SQL Database')
db = sqlite3.connect('cgc.sqlite')
c = db.cursor()
write_db(db, path_sql_file)
c.close()
db.close()
print('Database creation complete')