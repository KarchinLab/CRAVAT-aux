
import os
import re
import pprint

def log(s,f):
    f.write(s+'\n')
    print s

def get_h2c_match(group, header):
    out = []
    for row in h2c_rows:
        if row[0] == module_group and row[2] == header:
            out.append(row)
    return out

def read_and_split(f,delimiter,comment_char):
    out = []
    for line in f:
        if not(line.startswith(comment_char)):
            out.append(line.strip().split(delimiter))
    return out

valid_modules = ['info',
                 'vcfinfo',
                 'mappability',
                 'dbsnp',
                 'thousandgenomes',
                 'esp',
                 'exac',
                 'count',
                 'cosmic',
                 'geneannotator',
                 'mupit',
                 'vogelstein',
                 'target',
                 'clinvar',
                 'cgc',
                 'ndex_module'
                 ]
job_dir = os.path.join(os.curdir,'kmoad_20161207_170437')
module_groups = ['variant','gene','noncoding','summary']
used_rows = []
module_header_dicts = {}
log_file = open('gen_module_header_dicts_log.txt','w')

with open(os.path.join(os.curdir,'Interactive_result_header_to_column_table_data.txt')) as f:
    h2c_rows = read_and_split(f,'\t','#')

raw_headers = {}
for module_group in module_groups:
    group_dir = os.path.join(job_dir, module_group)
    module_names = os.listdir(group_dir)
    for module_name in module_names:
        if module_name not in raw_headers.keys():
            raw_headers[module_name] = {}
        with open(os.path.join(group_dir,module_name)) as f:
            f_as_list = read_and_split(f,'\t','#')
            if module_name == 'info':
                raw_headers[module_name][module_group] = f_as_list[0]
            else:
                raw_headers[module_name][module_group] = f_as_list[0][1:]
log('%sRaw Headers%s' %('='*15,'='*15), log_file)
log(pprint.pformat(raw_headers), log_file)
#example = {'module_group':{'header'::{'column_group':'Example Group','sql_col':'example_col', 'value_type':'varchar(400)', 'hidden': 0, 'column_order': 0}}}}
for module_name in raw_headers:
    log('%s%s%s' %('='*15,module_name,'='*15), log_file)
    module_headers = raw_headers[module_name]
    header_dict = {}
    for module_group in module_headers:
        log(module_group, log_file)
        group_headers = module_headers[module_group]
        header_dict[module_group] = {}
        for header in group_headers:
            log('\t%s' %header, log_file)
            matching_rows = get_h2c_match(module_group, header)
            if matching_rows:
                if len(matching_rows) > 1:
                    raise BaseException('Header %s matches multiple rows in header_to_col.' %header)
                h2c_row = matching_rows[0]
                log('\t%s' %h2c_row, log_file)
                used_rows.append(h2c_row)
                column_group = h2c_row[1]
                sql_col = h2c_row[3]
                sql_type = h2c_row[4]
                hidden = int(h2c_row[5])
                order = int(h2c_row[6])
                header_dict[module_group][header] = {'column_group': column_group,
                                                     'sql_col': sql_col,
                                                     'value_type': sql_type,
                                                     'hidden': hidden,
                                                     'column_order': order
                                                     }
            else:
                log('\t\tNO MATCH IN HEADER_TO_COL', log_file)
    log(pprint.pformat(header_dict), log_file)
    module_header_dicts[module_name] = header_dict

print '\n','='*25                
print 'Remaining Rows:'
for row in h2c_rows:
    if row not in used_rows:
        print '\t'.join(row)
print '%d rows used of %d, %d remaining.' %(len(used_rows),len(h2c_rows),len(h2c_rows)-len(used_rows))
log_file.close()

