import MySQLdb
import traceback
import shutil
import os
import argparse
import re
import sys

result_table_suffixes = ['_error','_gene','_noncoding','_variant']
all_data_dir = '/ext/jobs'

parser = argparse.ArgumentParser()
parser.add_argument('-i',
					dest='include',
					default='.*',
					help='Job Ids to delete (RegEx)')
parser.add_argument('-e',
					dest='exclude',
					default='',
					help='Job ids to keep (RegEx)')
cmd_args = parser.parse_args(sys.argv[1:])
include_re = re.compile(cmd_args.include)
exclude_re = re.compile(cmd_args.exclude)

try:
	db = MySQLdb.connect(host='localhost',
						 user='root', 
						 passwd='1',
						 db='cravat_results')
	c = db.cursor()
except:
	sys.exit('Failed to connect to database')

q = 'select job_id from cravat_admin.cravat_log where stop_time is not null;'
c.execute(q)
all_job_ids = [x[0] for x in c.fetchall()]
if not(all_job_ids):
	sys.exit('No completed jobs in cravat_admin.cravat_log')
delete_que = []
for job_id in all_job_ids:
	delete_it = bool(include_re.match(job_id))
	if cmd_args.exclude:
		if exclude_re.match(job_id):
			delete_it = False
	if delete_it:
		delete_que.append(job_id)

if delete_que:
	print('The following jobs will be deleted:\n%s' %(', '.join(delete_que)))
	cont = raw_input('Continue? <y/n>\n>>>')
	if cont not in ['y','Y']:
		sys.exit('Cancelled')
else:
	sys.exit('No matching jobs')
	
print('Deleting jobs')
for job_id in delete_que:
	for suffix in result_table_suffixes:
		try:
			table_name = job_id + suffix
			c.execute('drop table cravat_results.%s' %table_name)
		except:
			print('%s: failed to drop table %s' %(job_id, table_name))
	try:
		c.execute('delete from cravat_admin.cravat_log where job_id="%s";' %job_id)
		db.commit()
	except:
		print('%s: failed to drop row from cravat_log' %job_id)
	data_dir = os.path.join(all_data_dir, job_id)
	if os.path.exists(data_dir):
		if os.path.isdir(data_dir):
			try:
				shutil.rmtree(data_dir)
			except:
				print('%s: failed to delete data at %s' %(job_id, data_dir))
		else:
			print('%s: no directory at %s' %(job_id, data_dir))
	else:
		print('%s: no directory at %s' %(job_id, data_dir))