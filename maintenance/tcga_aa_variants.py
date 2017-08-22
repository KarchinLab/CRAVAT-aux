import os
import sys
import requests
import json
import time
import MySQLdb
import re

h2c = {'tissue':18,
       'chr':1,
       'start':2,
       'strand':4,
       'ref':7,
       'alt':9,
       'sample':10}

cravat_url_base = 'http://karchin-web02.icm.jhu.edu/CRAVAT'
cravat_user_email = 'kmoad@insilico.us.com'

def count_lines(fpath):
    with open(fpath) as f:
        n = 0
        for l in f:
            n += 1
    return n

def format_chr(chr):
    if not(chr.startswith('chr')):
       chr = 'chr' + chr
    if chr.endswith('x') or chr.endswith('y'):
        chr = chr[:-1] + chr[-1].upper()
    return chr 

def make_cravat_input(data_path, out_path):
    num_lines = count_lines(data_path)
    total_digits = len(str(num_lines))
    uid2chrom = {}
    with open(data_path,'rU') as f, open(out_path,'w') as wf:
        lnum = 0
        for l in f:
            lnum += 1
            cravat_list = [None]*7
            toks = l.strip('\n').split('\t') # learn a better way of stripping line endings
            tissue = toks[h2c['tissue']]
            lnum_digits = len(str(lnum))
            num_lead_zeros = total_digits - lnum_digits
            uid = '%s%d-%s' %('0'*num_lead_zeros, lnum, tissue)
            cravat_list[0] = uid
            cravat_list[1] = format_chr(toks[h2c['chr']])
            cravat_list[2] = toks[h2c['start']]
            cravat_list[3] = toks[h2c['strand']]
            cravat_list[4] = toks[h2c['ref']]
            cravat_list[5] = toks[h2c['alt']]
            cravat_list[6] = toks[h2c['sample']]
            wf.write('\t'.join(cravat_list) + '\n')
            uid2chrom[uid] = format_chr(toks[h2c['chr']])
            
def submit_to_cravat(input_path):
        data = {'email': cravat_user_email,'analyses': ''}
        files = {'inputfile': open(input_path, 'r')}
        print cravat_url_base+'/rest/service/submit'
        print files
        print data
        r = requests.post(cravat_url_base+'/rest/service/submit', files=files, data=data)
        job_id = json.loads(r.text)['jobid']
        return job_id

def wait_for_completion(job_id, sleep_time):
    job_status = ''
    while job_status == '':
        try:
            json_response = requests.get('%s/rest/service/status?jobid=%s' %(cravat_url_base, job_id))
            json_status = json.loads(json_response.text)['status']
        except Exception, e:
            print e
            time.sleep(sleep_time)
            continue
        if json_status in ['Success', 'Salvaged', 'Error']:
            job_status = json_status
        else:
            time.sleep(sleep_time)
    return job_status

def fetch_results(dbconn, variants_table):
    c = dbconn.cursor()
    q = 'select * from cravat_results.%s;' %variants_table
    c.execute(q)
    headers = [x[0] for x in c.description]
    qr = c.fetchmany()
    while qr:
        d = dict(zip(headers,qr[0]))
#         for i,h in enumerate(headers):
#             d[h] = row[i]
        yield d
        qr = c.fetchmany()
    c.close()

def get_uniprot_transc_from_hugo(dbconn,hugo):
    c = dbconn.cursor()
    q = 'select distinct cravat_transcript from annot_liftover.uniprot_cravat_conversion where hugo="%s";' %hugo
    c.execute(q)
    return q, c.fetchall()

def write_mysql_data(dbconn, job_id, out_path):
    c = dbconn.cursor()
    transc_aa_re = re.compile('(.*):([A-Z]|_|\*)(\d+)([A-Z]|_|\*)\((.*)\)')
    variants_table = job_id + '_variant'
    noncoding_table = job_id + '_noncoding'
    error_table = job_id + '_error'
    q = 'select count(*) from cravat_results.%s;' %error_table
    c.execute(q)
    print 'Errors:', str(c.fetchone()[0])
    q = 'select count(*) from cravat_results.%s' %noncoding_table
    c.execute(q)
    print 'Noncoding:', str(c.fetchone()[0])
    with open(out_path,'w') as wf:
        num_uniprot_match_fail = 0
        num_uniprot_no_transc = 0
        num_mult_transc = 0
        no_transc_queries = set([])
        mult_transc_queries = set([])
        for result_dict in fetch_results(dbconn, variants_table):
            out_list = [None]*8
            out_list[0] = result_dict['uid'].split('-')[1]
            out_list[1] = result_dict['hugo']
            uniprot_query, uniprot_query_result = get_uniprot_transc_from_hugo(dbconn, result_dict['hugo'])
            if not(uniprot_query_result):
                num_uniprot_no_transc += 1
                no_transc_queries.add(uniprot_query)
                continue
            elif len(uniprot_query_result) > 1:
                num_mult_transc += 1
                mult_transc_queries.add(uniprot_query)
                continue
            else:
                transc_from_uniprot = uniprot_query_result[0][0]
            all_transc_aa = result_dict['so_best_all'].split(',')
            transc_from_uniprot_found = False
            for transc_aa in all_transc_aa:
                if not(transc_from_uniprot_found):
                    if transc_aa.startswith('*'):
                        transc_aa = transc_aa[1:]
                    transc_aa_match = transc_aa_re.match(transc_aa)
                    transc = transc_aa_match.groups()[0]
                    if transc == transc_from_uniprot:
                        transc_from_uniprot_found = True
                        out_list[2] = transc
                        out_list[3] = transc_aa_match.groups()[1]
                        out_list[4] = transc_aa_match.groups()[3]
                        out_list[5] = transc_aa_match.groups()[2]
                        out_list[6] = transc_aa_match.groups()[4]
            if not(transc_from_uniprot_found):
                num_uniprot_match_fail += 1
                continue
#                 raise Exception('Transc in uniprot_cravat_conversion not present in CRAVAT results for uid: %s' %result_dict['uid'])
            out_list[7] = str(result_dict['num_sample'])
            wf.write('\t'.join(out_list) + '\n')
    print 'Uniprot transc not available: %d' %num_uniprot_no_transc
    print 'Uniprot transc not in results: %d' %num_uniprot_match_fail
    c.close()
    
def get_uniprot_table_transc_length(dbconn, transc):
    try:
        q = 'select aa_len from annot_liftover.uniprot_cravat_conversion where cravat_transcript = "%s";' %transc
        c = dbconn.cursor()
        c.execute(q)
        return c.fetchone()[0]
    except Exception, e:
        raise e 
            
def write_mysql_data_from_info(count_path, info_path, out_path, dbconn):
    transc_aa_re = re.compile('(.*):([[A-Z,\*,\$,\_,\?,\-]{0,})(\d+)([[A-Z,\*,\$,\_,\?,\-]{0,})\((.*)\)')
    aa_pos_re = re.compile('\d+')
    
    print 'Making num_identical_vars dict'
    with open(info_path) as f:
        headers = f.readline().strip().split('\t')
        num_identical_vars = {}
        for l in f:
            toks = l.strip().split()
            td = dict(zip(headers,toks))
            uid = td['ID'].split('-')[1]
            var = ','.join([td['Chrom'], td['Position'], td['Strand'], td['Reference base(s)'], td['Alternate base(s)']])
            if var in num_identical_vars:
                num_identical_vars[var] += 1
            else:
                num_identical_vars[var] = 1
    
    print 'Reading from info file'    
    with open(info_path) as f, open(out_path,'w') as wf:
        headers = f.readline().strip().split('\t')
        num_minus_strands = 0
        num_aa_fail = 0
        num_coding = 0
        num_non_coding = 0
        num_total = 0
        num_uniprot_no_transc = 0
        num_uniprot_mult_transc = 0
        num_uniprot_match_fail = 0
        no_uniprot_transc_queries = set([])
        mult_uniprot_transc_queries = set([])
        minus_strand_uids = set([])
        for l in f:
            num_total += 1
            if num_total % 10000 == 0:
                print num_total
            toks = l.strip().split('\t')
            td = dict(zip(headers,toks))
            try:
                out_list = [None]*8
                if td['HUGO symbol'] != 'Non-Coding':
                    num_coding += 1
                    if td['Strand'] == '-':
                        num_minus_strands += 1
                        minus_strand_uids.add(td['ID'])
                    out_list[0] = td['ID'].split('-')[1]
                    out_list[1] = td['HUGO symbol']
                    out_list[2] = td['S.O. transcript']
                    
                    uniprot_query, uniprot_query_result = get_uniprot_transc_from_hugo(dbconn, td['HUGO symbol'])
                    if not(uniprot_query_result):
                        num_uniprot_no_transc += 1
                        no_uniprot_transc_queries.add(uniprot_query)
                        continue
                    elif len(uniprot_query_result) > 1:
                        transc_lengths = {}
                        cur_max_length = 0
                        longest_transc = ''
                        for row in uniprot_query_result:
                            transc = row[0]
                            transc_length = get_uniprot_table_transc_length(dbconn, transc)
                            if transc_length > cur_max_length:
                                longest_transc = transc
                        transc_from_uniprot = longest_transc
                    else:
                        transc_from_uniprot = uniprot_query_result[0][0]
                        
                    all_transc_aa = td['All transcripts'].split(',')
                    transc_from_uniprot_found = False
                    for transc_aa in all_transc_aa:
                        if not(transc_from_uniprot_found):
                            if transc_aa.startswith('*'):
                                transc_aa = transc_aa[1:]
                            transc_aa_match = transc_aa_re.match(transc_aa)
                            match_groups = transc_aa_match.groups()
                            transc = match_groups[0]
                            if transc == transc_from_uniprot:
                                transc_from_uniprot_found = True
                                out_list[2] = transc
                                if match_groups[1]:
                                    out_list[3] = match_groups[1]
                                else:
                                    out_list[3] = '_'
                                if match_groups[2]:
                                    out_list[5] = match_groups[2]
                                else:
                                    out_list[5] = '_'
                                out_list[4] = match_groups[3]
                                out_list[6] = match_groups[4]
                    if not(transc_from_uniprot_found):
                        num_uniprot_match_fail += 1
                        continue
                    var = ','.join([td['Chrom'], td['Position'], td['Strand'], td['Reference base(s)'], td['Alternate base(s)']])
                    out_list[7] = str(num_identical_vars[var])
                    wf.write('\t'.join(out_list) + '\n')
                else:
                    num_non_coding += 1
            except Exception, e:
                print td
                raise e
                
        print 'Num minus strands:', num_minus_strands
        print 'Num aa fail:', num_aa_fail
        print 'Num total:', num_total
        print 'Num coding:', num_coding
        print 'Num non coding:', num_non_coding
        print 'Uniprot query returns nothing:', num_uniprot_no_transc
        print 'Uniprot query returns multiple:', num_uniprot_mult_transc
        print 'Uniprot transc not in results:', num_uniprot_match_fail
        print 'Uniprot fail queries:\n\t%s' %'\n\t'.join(no_uniprot_transc_queries)
        print 'Uniprot mult queries:\n\t%s' %'\n\t'.join(mult_uniprot_transc_queries)

if __name__ == '__main__':
    data_path = os.path.abspath(sys.argv[1])
    working_dir = os.path.dirname(data_path)
    cravat_path = data_path + '.cravat'
    mysql_data_path = os.path.join(working_dir,'tcga_aa_variants_mysqldata.tsv')
    info_path = os.path.join(working_dir,'info')
    count_path = os.path.join(working_dir,'count')
    
    print 'TCGA data:', data_path
    
#     print 'Converting to CRAVAT input'
#     make_cravat_input(data_path, cravat_path)
#     print 'CRAVAT input:', cravat_path
#     print 'Submitting to CRAVAT'
#     job_id = submit_to_cravat(cravat_path)
#     print 'Job id:', job_id
#     print 'Waiting for job completion'
#     job_status = wait_for_completion(job_id, 1)
#     print 'Job completed with status:', job_status

#     job_id = 'kmoad_20170606_135909'
    
    print 'Fetching data'
    dbconn_local = MySQLdb.connect(host='localhost',user='root',passwd='1234')
#     dbconn_cravat = MySQLdb.connect(host='karchin-web02.icm.jhu.edu', port=8895, user='root', passwd='1')
#     write_mysql_data(dbconn_cravat, job_id, mysql_data_path) # Fetches from the database
    write_mysql_data_from_info(count_path, info_path, mysql_data_path, dbconn_local) # Fetches from the info file 
    print 'MySQL input:', mysql_data_path