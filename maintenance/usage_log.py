import datetime
import MySQLdb
import os
import random
import sys

mysql_host = 'localhost'
mysql_user = 'cravat'
mysql_password = 'cravat_password'
db_name = 'CRAVAT'
table_name = 'cravat_log'
# analysis_type: 1 driver, 2 functional, 3 gene annotation only
table_scheme = 'server varchar(4), job_id varchar(100), email varchar(50), client_ip varchar(40), ' + \
               'no_input_line int, success boolean, ' + \
               'submit_time datetime, start_time datetime, stop_time datetime, run_time_in_second int,' + \
               'analysis_type int, chasm boolean, snvget boolean, vest boolean, ' + \
               'gene_annotation boolean, classifier varchar(20), ' + \
               'input_coordinate varchar(10), hg18 boolean, mupit_input boolean, ' + \
               'mutation_filename varchar(200), error_message text'

def create_log_table ():
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    cursor.execute('drop table if exists ' + table_name)
    cursor.execute('create table ' + table_name + '(' + table_scheme + ') engine=innodb')

def random_populate_log_table ():
    db = MySQLdb.connect(host=mysql_host,\
                         user=mysql_user,\
                         passwd=mysql_password,\
                         db=db_name)
    cursor = db.cursor()
    cursor.execute('truncate table ' + table_name)
    servers = ['dev', 'prod']
    emails = ['test@test.com', 'rand@rand.com', 'user@jhu.edu', 'cravat@insilico.us.com']
    analysis_types = ['driver', 'functional', 'geneannotationonly']
    driver_analyses = ['CHASM', 'SNVGet', 'CHASM_SNVGet']
    functional_analyses = ['VEST', 'SNVGet', 'VESTSNVGet']
    classifiers  = ['Colon', 'Liver', 'Stomach']
    input_coordinates = ['genomic', 'transcript']
    for i in xrange(10000):
        year = random.randint(2012,2012)
        month = random.randint(1,12)
        if month in [1, 3, 5, 7, 8, 10, 12]:
            day = random.randint(1,31)
        elif month in [4, 6, 9, 11]:
            day = random.randint(1,30)
        elif month in [2]:
            day = random.randint(1,28)
        hour = random.randint(0,23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        submit_time = str(year).rjust(4, '0') + '-' + str(month).rjust(2, '0') + '-' + str(day).rjust(2, '0') + ' ' + str(hour).rjust(2, '0') + ':' + str(minute).rjust(2, '0') + ':' + str(second).rjust(2, '0')
        submit_time_for_job_id = submit_time.replace('-', '').replace(':', '').replace(' ', '_')
        second = second + random.randint(1, 1000)
        if second >= 60:
            minute = minute + (second / 60)
            second = second % 60
        if minute >= 60:
            hour = hour + (minute / 60)
            minute = minute % 60
        if hour >= 24:
            day = day + (hour / 24)
            hour = hour % 24
        if month in [1, 3, 5, 7, 8, 10, 12]:
            if day > 31:
                month += 1
                day = 1
        elif month in [4, 6, 9, 11]:
            if day > 30:
                month += 1
                day = 1
        elif month in [2]:
            if day > 28:
                month += 1
                day = 1
        if month > 12:
            year += 1
            month = 1
        start_time = str(year).rjust(4, '0') + '-' + str(month).rjust(2, '0') + '-' + str(day).rjust(2, '0') + ' ' + str(hour).rjust(2, '0') + ':' + str(minute).rjust(2, '0') + ':' + str(second).rjust(2, '0')
        print 'start_time=',start_time
        start_datetime = datetime.datetime(year, month, day, hour, minute, second)
        second = second + random.randint(1, 1000)
        if second >= 60:
            minute = minute + (second / 60)
            second = second % 60
        if minute >= 60:
            hour = hour + (minute / 60)
            minute = minute % 60
        if hour >= 24:
            day = day + (hour / 24)
            hour = hour % 24
        if month in [1, 3, 5, 7, 8, 10, 12]:
            if day > 31:
                month += 1
                day = 1
        elif month in [4, 6, 9, 11]:
            if day > 30:
                month += 1
                day = 1
        elif month in [2]:
            if day > 28:
                month += 1
                day = 1
        if month > 12:
            year += 1
            month = 1
        stop_time = str(year).rjust(4, '0') + '-' + str(month).rjust(2, '0') + '-' + str(day).rjust(2, '0') + ' ' + str(hour).rjust(2, '0') + ':' + str(minute).rjust(2, '0') + ':' + str(second).rjust(2, '0')
        print 'year=',year,'month=',month,'day=',day,'hour=',hour,',minute=',minute,'second=',second
        stop_datetime = datetime.datetime(year, month, day, hour, minute, second)
        run_datetime = stop_datetime - start_datetime
        run_time_in_second = str(run_datetime.days * 60 * 60 * 24 + run_datetime.seconds)
        server = servers[random.randint(0, len(servers) - 1)]
        email = emails[random.randint(0, len(emails) - 1)]
        job_id = email.split('@')[0] + '_' + submit_time_for_job_id
        no_input_line = str(random.randint(10, 10000))
        success = random.randint(0,10)
        if success >= 1:
            success = '1'
        else:
            success = '-1'
        analysis_type = str(random.randint(1, len(analysis_types)))
        if analysis_type == '1': # driver
            chasm = str(random.randint(0,1))
            snvget = str(random.randint(0,1))
            vest = '0'
            gene_annotation = str(random.randint(0,1))
        elif analysis_type == '2': # functional
            chasm = '0'
            snvget = str(random.randint(0,1))
            vest = str(random.randint(0,1))
            gene_annotation = str(random.randint(0,1))
        else:
            chasm = '0'
            snvget = '0'
            vest = '0'
            gene_annotation = '1'
        if chasm == '1':
            classifier = classifiers[random.randint(0, len(classifiers) - 1)]
        else:
            classifier = 'NA'
        input_coordinate = input_coordinates[random.randint(0, len(input_coordinates) - 1)]
        hg18 = str(random.randint(0,1))
        error_message = 'NA'
        stmt = 'insert into ' + table_name + ' (' + \
               'server, job_id, email, no_input_line, success, submit_time, ' + \
               'start_time, stop_time, run_time_in_second, analysis_type, chasm, snvget, vest, ' + \
               'gene_annotation, classifier, input_coordinate, hg18, ' + \
               'error_message) values ("' + server + '", "' + job_id + '", "' + \
               email + '", ' + no_input_line + ', ' + success + ', "' + \
               submit_time + '", "' + start_time + '", "' + stop_time + '", ' + run_time_in_second + ', ' + \
               analysis_type + ', ' + chasm + ', ' + snvget + ', ' + vest + ', ' + gene_annotation + \
               ', "' + classifier + '", "' + input_coordinate + '", ' + hg18 + \
               ', "' + error_message + '")'
        print stmt
        cursor.execute(stmt)
    db.commit()

if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'create':
        create_log_table()
    elif cmd == 'populate':
        random_populate_log_table()