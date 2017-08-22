import MySQLdb
import re
import os
import traceback
import subprocess
import cmd

print 'Working in '+os.getcwd()
dbconn = MySQLdb.connect(host='karchin-web02.icm.jhu.edu',port=8895,user='root',passwd='1')
c = dbconn.cursor()


select_cols = ['uid',
               'chrom',
               'position',
               'strand',
               'ref_base',
               'alt_base',
               'vest_all',
               'so_best_all']

h2c = dict(zip(select_cols,range(len(select_cols))))

base_rev_dict = {'A':'T','T':'A','G':'C','C':'G'}
chrom_id_re = re.compile('(chr[\dxXyYuUnN]{1,2})(.*)_\d+_')
# aa_change_re = re.compile('([A-z\*]+)(\d+)([A-z\*]+)')
vest_result_re = re.compile('(.*):.*\(([\d\.]+)')
so_result_re = re.compile('(.*):([A-z\*]+)?(\d+)([A-z\*]+)?')

chrom_files = {}
main_chroms = ['chry', 'chrx', 'chr13', 'chr12', 'chr11', 'chr10', 'chr17', 'chr16', 
               'chr15', 'chr14', 'chr19', 'chr18', 'chr22', 'chr20', 'chr21', 'chr7', 
               'chr6', 'chr5', 'chr4', 'chr3', 'chr2', 'chr1', 'chr9', 'chr8', 'chrUn']
for chrom in main_chroms:
    fname = 'vest_precompute_'+chrom+'.txt'
    fpath = os.path.abspath(fname)
    if os.path.exists(fpath):
        os.remove(fpath)
    chrom_files[chrom] = open(fpath,'w')    

q = """select job_id from cravat_admin.cravat_log 
where email like "mryan%" 
and mutation_filename like "%vest\_chr%\_hg38.txt%" 
and result_tables like "_%";"""
c.execute(q)
qr = c.fetchall()
jobs = [x[0] for x in qr]
i_jobs = 0
        
for job_id in jobs:
    i_jobs += 1
    print 'Job %d of %d, ID: %s' %(i_jobs, len(jobs), job_id)
    variant_tablename = job_id + '_variant'
    cmd = 'mysqldump -u root -p1 cravat_results %s > /ext/temp/vest_precompute_jobs/%s.sql' %(variant_tablename, variant_tablename)
    print cmd
    subprocess.call(cmd.split())