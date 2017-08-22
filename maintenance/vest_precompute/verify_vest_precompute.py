import MySQLdb
import re
import os
import traceback
from _sqlite3 import Row

print 'Working in '+os.getcwd()
dbconn = MySQLdb.connect(host='karchin-web02.icm.jhu.edu',port=8895,user='root',passwd='1')
c = dbconn.cursor()

main_chroms = ['chry', 'chrx', 'chr13', 'chr12', 'chr11', 'chr10', 'chr17', 'chr16', 
               'chr15', 'chr14', 'chr19', 'chr18', 'chr22', 'chr20', 'chr21', 'chr7', 
               'chr6', 'chr5', 'chr4', 'chr3', 'chr2', 'chr1', 'chr9', 'chr8', 'chrUn']
expected_counts = dict.fromkeys(main_chroms, 0)

# q = """select job_id from cravat_admin.cravat_log 
# where email like "mryan%" 
# and mutation_filename like "%vest\_chr%\_hg38.txt%" 
# and result_tables like "_%";"""
# c.execute(q)
# qr = c.fetchall()
# jobs_query = [x[0] for x in qr]

with open('jobs_list.txt') as f:
    jobs = [l.strip() for l in f]
    
chrom_id_re = re.compile('(chr[\dxXyYuUnN]{1,2})(.*)_\d+_')

i_jobs = 0
for job_id in jobs:
    i_jobs += 1
    print 'Job %d of %d, ID: %s' %(i_jobs, len(jobs), job_id)
    variant_tablename = job_id + '_variant'
    q = 'select uid, vest_all from cravat_results.%s' %variant_tablename
    c.execute(q)
    for row in c.fetchall():
        try:
            chrom_match = chrom_id_re.search(row[0])
            chrom_primary = chrom_match.group(1).lower()
            if row[1]:
                expected_counts[chrom_primary] += row[1].count(',')
        except:
            print row
            raise
print 'expected_counts:', expected_counts

for chrom in expected_counts:
    q = 'select count(*) from vest_precompute.vest_precompute_%s;' %chrom
    c.execute(q)
    count_actual = c.fetchone()[0]
    counts_match = count_actual == expected_counts[chrom]
    print chrom, expected_counts[chrom], count_actual, counts_match