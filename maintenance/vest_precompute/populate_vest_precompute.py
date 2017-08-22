import MySQLdb
import re
import os
import traceback

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
    q = 'select %s from cravat_results.%s' %(', '.join(select_cols), variant_tablename)
    c.execute(q)
    qr = c.fetchall()
    i_rows = 0
    for r in qr:
        if i_rows%50000 == 0:
            pass
            print '\tRow %d of %d' %(i_rows, len(qr))
        i_rows += 1
        
        rd = dict(zip(select_cols,r))
        
        try:
            chrom_match = chrom_id_re.search(rd['uid'])
            chrom_primary, chrom_extra = chrom_match.groups()
            chrom_full = chrom_primary + chrom_extra
            
            strand = rd['strand']
            ref_nt = ''
            alt_nt = ''
            if strand == '+':
                ref_nt = rd['ref_base']
                alt_nt = rd['alt_base']
            else:
                ref_nt = base_rev_dict[rd['ref_base']]
                alt_nt = base_rev_dict[rd['alt_base']]
            
            if not(rd['vest_all']): continue
            so_hits = rd['so_best_all'].split(',')
            transc2pchange = {}
            for so_result in so_hits:
                if so_result.startswith('*'):
                    so_result = so_result[1:]
                so_result_match = so_result_re.match(so_result)
                transc, ref_aa, position, alt_aa = so_result_match.groups()
                transc2pchange[transc] = {'ref':ref_aa, 'alt':alt_aa, 'pos':position}
            vest_hits = rd['vest_all'].split(',')
            for vest_result in vest_hits:
                if vest_result.startswith('*'):
                    vest_result = vest_result[1:]
                vest_result_match = vest_result_re.match(vest_result)
                transc, vest_score = vest_result_match.groups()
                ref_aa = transc2pchange[transc]['ref']
                alt_aa = transc2pchange[transc]['alt']
                aa_pos = transc2pchange[transc]['pos']
                transc_aa = '%s:%s%s%s' %(transc, ref_aa, aa_pos, alt_aa)
                toks_out = [chrom_full, rd['position'], ref_nt, alt_nt, ref_aa, alt_aa, vest_score, transc_aa]
                chrom_files[chrom_primary].write('\t'.join(map(str,toks_out))+'\n')
        except Exception, e:
            print rd
            print traceback.format_exc()
            break
            
# +---------------------+--------------+------+-----+---------+-------+
# | Field               | Type         | Null | Key | Default | Extra |
# +---------------------+--------------+------+-----+---------+-------+
# | chrom               | varchar(5)   | YES  |     | NULL    |       |
# | pos                 | int(11)      | YES  | MUL | NULL    |       |
# | ref_nt              | char(1)      | YES  |     | NULL    |       |
# | alt_nt              | char(1)      | YES  |     | NULL    |       |
# | ref_aa              | char(1)      | YES  |     | NULL    |       |
# | alt_aa              | char(1)      | YES  |     | NULL    |       |
# | score               | float        | YES  |     | NULL    |       |
# | transcript_aachange | varchar(100) | YES  |     | NULL    |       |
# +---------------------+--------------+------+-----+---------+-------+

for chrom in chrom_files:
    chrom_files[chrom].close()
c.close()
dbconn.close()