import datetime
import MySQLdb
import os
import random
import sys

mysql_host = 'localhost'
mysql_user = 'cravat'
mysql_password = 'cravat_password'
db_name = 'CRAVAT'
table_name = 'database_version'

table_scheme = 'db_name varchar(20), version varchar(30)'

def create_table ():
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    cursor.execute('drop table if exists ' + table_name)
    cursor.execute('create table ' + table_name + '(' + table_scheme + ') engine=innodb')
    db.commit()

def initial_population ():
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    valuess = [\
              {'db_name':'dbsnp', 'version':'137'}, \
              {'db_name':'cosmic', 'version':'v58'}, \
              {'db_name':'1000genomes', 'version':'Phase 1 release v3'}, \
              {'db_name':'esp6500', 'version':'v.0.0.13'}, \
              {'db_name':'snvbox', 'version':'2.0.0'}, \
              {'db_name':'genecards', 'version':'web_cache'}, \
              {'db_name':'pubmed', 'version':'web_cache'}\
             ]
    for values in valuess:
        cursor.execute('insert into '+table_name+' (db_name, version) values ("' + values['db_name'] + '", "' + values['version'] + '")')
    db.commit()

if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'create':
        create_log_table()
    elif cmd == 'initial_population':
        initial_population()