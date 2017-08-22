'''
Broad Institute's TARGAT database (https://www.broadinstitute.org/cancer/cga/target)
'''

import MySQLdb
db = MySQLdb.connect(host='karchin-db01.icm.jhu.edu', user='mryan', passwd='royalenfield', db='CRAVAT_ANNOT_DEV')
cursor = db.cursor()
cursor.execute('drop table if exists target')
db.commit()
cursor.execute('create table target (gene varchar(40), rationale varchar(400), agents_therapy varchar(100)) engine=innodb')
db.commit()
cursor.execute('load data local infile "target_list.txt" into table target (gene, rationale, agents_therapy) fields terminated by "\t"')
db.commit()