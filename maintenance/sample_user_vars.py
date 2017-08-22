import MySQLdb
import os
import random
import sys
import numpy as np

def get_tables_and_lengths(dbconn):
    c = dbconn.cursor()
    q = 'select table_name, table_rows from information_schema.tables where table_schema="cravat_results";'
    print q
    c.execute(q)
    qr = c.fetchall()
    tables = []
    lengths = []
    valid_types = ['noncoding', 'variant']
    banned_users = ['tdcasasent','kmoad','rkim','mryan','znylund','mstucky']
    for r in qr:
        if r[0] != 'header_to_col':
            table = r[0]
            length = r[1]
            user = table.split('_')[0]
            table_type = table.split('_')[-1]
            if length > 0 and table_type in valid_types and user not in banned_users:
                tables.append(table)
                lengths.append(length)
    c.close()
    return tables, lengths

if __name__ == '__main__':
    num_vars = int(sys.argv[1])
    out_path = sys.argv[2]
    
    dbconn = MySQLdb.connect(host='karchin-web04.icm.jhu.edu', port=8895, user='root', passwd='1')
    c = dbconn.cursor()
    wf = open(out_path,'w')
    select_cols = ['uid','chrom','position','strand','ref_base','alt_base']
    tables, lengths = get_tables_and_lengths(dbconn)
    tuids = []
    for table in tables:
        q = 'select uid from cravat_results.%s;' %table
        c.execute(q)
        qr = c.fetchall()
        for r in qr:
            tuids.append(table+'|'+r[0])
    chosen_tuids = np.random.choice(tuids,size=num_vars)
    uids_for_table = {}
    for tuid in chosen_tuids:
        table, uid = tuid.split('|')
        if uids_for_table.has_key(table):
            uids_for_table[table].append(uid)
        else:
            uids_for_table[table] = [uid]
    for table, uids in uids_for_table.iteritems():
        job_id = '_'.join(table.split('_')[:-1])
        q = 'select %s from cravat_results.%s where uid in ("%s");' %(', '.join(select_cols), table, '","'.join(uids))
        c.execute(q)
        qr = c.fetchall()
        for r in qr:
            wtoks = [job_id+'|'+r[0]] + list(r[1:]) + [job_id]
            wf.write('\t'.join(map(str,wtoks)) + '\n')

#     print len(tables)
#     sum_lengths = float(sum(lengths))
#     probs = [float(x)/sum_lengths for x in lengths]
#     chosen_tables = np.random.choice(tables, size=num_vars, replace=True, p=probs)
#     vars_from_table = {}
#     for table in chosen_tables:
#         if vars_from_table.has_key(table):
#             vars_from_table[table] += 1
#         else:
#             vars_from_table[table] = 0
#     for table, num_vars in vars_from_table.iteritems():
#         table_toks = table.split('_')
#         table_type = table_toks[-1]
#         job_id = '_'.join(table_toks[:-1])
#         q = 'select uid from cravat_results.%s;' %table
#         c.execute(q)
#         uids = [x[0] for x in c.fetchall()]
#         num_vars = min([num_vars, len(uids)])
#         chosen_uids = np.random.choice(uids, num_vars, replace=False)
#         select_cols = ['uid','chrom','position','strand','ref_base','alt_base']
#         q = 'select %s from cravat_results.%s where uid in ("%s");' %(', '.join(select_cols), table, '","'.join(chosen_uids))
#         c.execute(q)
#         qr = c.fetchall()
#         for r in qr:
#             wtoks = [job_id+r[0]] + list(r[1:]) + [job_id]
#             wf.write('\t'.join(map(str,wtoks)) + '\n')