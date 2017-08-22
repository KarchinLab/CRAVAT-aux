import MySQLdb
import re
import os
import traceback

print 'Working in '+os.getcwd()
dbconn = MySQLdb.connect(host='karchin-web02.icm.jhu.edu',port=8895,user='root',passwd='1')
c = dbconn.cursor()

files = os.listdir(os.getcwd())
for fname in files:
    fpath = os.path.abspath(fname)
    if fname.endswith('.txt'):
        chrom = fname.split('.')[0].split('_')[-1]
        q = 'load data infile "%s" into table vest_precompute.vest_precompute_%s;' %(fpath, chrom)
        print q
        c.execute(q)      
    