'''
Adds Pfam annotation to uniprot_cravat_conversion table, which
should have already been built.

Rick Kim
6/14/2017
'''

import MySQLdb
import os

db = MySQLdb.connect('localhost', 'root', '1', 'cravat_annot')
cursor = db.cursor()

uniprot_filename = '/ext/temp/uniprot_ids.txt'
uniprot_ids = {}
if os.path.exists(uniprot_filename):
    print 'Reading uniprot_ids file...'
    f = open(uniprot_filename)
    for line in f:
        [uniprot_id, chrom, aa_len, cravat_transcript, hugo] = line.strip().split('\t')
        if uniprot_ids.has_key(uniprot_id) == False:
            uniprot_ids[uniprot_id] = []
        uniprot_ids[uniprot_id].append([chrom, aa_len, cravat_transcript, hugo])
    f.close()
else:
    print 'Making uniprot_ids file...'
    cursor.execute('select distinct chrom, uniprot_id, aa_len, cravat_transcript, hugo from uniprot_cravat_conversion')
    results = cursor.fetchall()
    wf = open(uniprot_filename, 'w')
    for result in results:
        [chrom, uniprot_id, aa_len, cravat_transcript, hugo] = result
        wf.write('\t'.join([uniprot_id, chrom, str(aa_len), cravat_transcript, hugo]) + '\n')
        if uniprot_ids.has_key(uniprot_id) == False:
            uniprot_ids[uniprot_id] = []
        uniprot_ids[uniprot_id].append([chrom, str(aa_len), cravat_transcript, hugo])
    wf.close()

def get_pfam_chunk ():
    line = f.readline().strip()
    if line == '':
        return None, None, None
    while line != '# STOCKHOLM 1.0':
        line = f.readline().strip()
    line = f.readline().strip()
    while line != '//' and line != '':
        field = line[5:7]
        if field == 'AC':
            pfam_id = line[10:].split('.')[0]
        elif field == 'DE':
            desc = line[10:]
        elif field == 'TP':
            feature_key = line[10:]
        line = f.readline().strip()
    return pfam_id, feature_key, desc

print 'Reading Pfam file...'
f = open('Pfam-A.hmm.dat')
pfams = {}
while True:
    pfam_id, feature_key, desc = get_pfam_chunk()
    if pfam_id == None:
        break
    feature_key = feature_key.lower()
    pfams[pfam_id] = [feature_key, desc]
print '[', f.readline(), ']'
f.close()

print 'Processing Pfam UniProt file...'
f = open('Pfam-A.regions.uniprot.tsv')
wf = open('pfam_uniprot_cravat_conversion.txt', 'w')
count = 0
f.readline()
for line in f:
    count += 1
    if count % 100000 == 0:
        print count
    toks = line.strip().split()
    uniprot_id = toks[0]
    if uniprot_ids.has_key(uniprot_id):
        pfam_id = toks[4]
        start = toks[5]
        stop = toks[6]
        if pfams.has_key(pfam_id) == False:
            print pfam_id, 'not in pfams'
            exit()
        [feature_key, feature_desc] = pfams[pfam_id]
        uniprot_datas = uniprot_ids[uniprot_id]
        for uniprot_data in uniprot_datas:
            [chrom, aa_len, cravat_transcript, hugo] = uniprot_data
            wf.write('\t'.join([chrom, feature_key, feature_desc, start, stop, \
                      uniprot_id, aa_len, cravat_transcript, hugo, 'Pfam']) + '\n')
wf.close()
f.close()
