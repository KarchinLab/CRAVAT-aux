import os
import shutil
import sys
import time
from pyres import ResQ

server_type = 'dev'
queue_name_prefix = 'CRAVAT_DEV'
cravat_dir = '/export/karchin-web02/CRAVAT_DEV/tomcat/webapps/CRAVAT/'
wrappers_dir = os.path.join(cravat_dir, 'Wrappers')
diagnostic_tests_dir = os.path.join(cravat_dir, 'diagnostic_tests')
sys.path.append(wrappers_dir)

hg18_options = ['off', 'on']
transcript_types = ['NM', 'NP', 'CCDS', 'ENST', 'ENSP']
analysis_types = ['driver', 'functional', 'geneannotationonly']
analysis_programs_by_analysis_type = {'driver': ['CHASM', 'SnvGet', 'CHASM_SnvGet'], \
                                       'functional': ['VEST', 'SnvGet', 'VEST_SnvGet'], \
                                       'geneannotationonly': ['NA']}

import masteranalyzer

job_ids = []

def queue_job(input_coordinate=None, \
              hg18_option='off', \
              transcript_type='NA', \
              refseq_correspondence='no', \
              sample_id='yes', \
              analysis_type=None, \
              analysis_programs=None):
    if analysis_type == None:
        print 'Choose an analysis type.'
        return
    input_basename = input_coordinate + '_' + hg18_option + '_' + transcript_type + '_' + refseq_correspondence + '_' + sample_id + '.txt'
    job_name = input_basename[:-4] + '_' + analysis_type + '_' + analysis_programs + '_20121231_010101'
    job_dir = os.path.join(diagnostic_tests_dir, job_name)
    if os.path.exists(job_dir) == False:
        os.mkdir(job_dir)
    input_filename = os.path.join(job_dir, input_basename)
    shutil.copy(os.path.join(diagnostic_tests_dir, input_basename), input_filename)
    job_id = job_name
    mutation_filename = input_filename
    mutation_filename_fix = mutation_filename + '.fix'
    mutation_filename_error = mutation_filename +'.error'
    email = 'cravattest@gmail.com'
    classifier = '_Other'
    upload_filename = input_basename
    user_upload_dir = job_dir
    chosen_db_str = analysis_programs.strip('_')
    chosen_dbs = chosen_db_str.split('_')
    tsv_report = 'on'
    gene_annot = 'on'
    hg18 = hg18_option
    functional_annot = 'on'
    f = open(input_filename)
    no_input_line = 0
    for line in f:
        if len(line) > 0:
            if not line[0] in ['#', '>', '!']:
                no_input_line += 1
    f.close()
    if no_input_line >= 5000:
        queue_name = queue_name_prefix + '_LARGE'
    else:
        queue_name = queue_name_prefix + '_SMALL'
    mupit_input = 'on'
    resubmit = 'no'
    wf = open(os.path.join(job_dir, 'job_info.txt'), 'w')
    wf.write(str(no_input_line)+'\n')
    wf.write(job_id+'\n')
    wf.write(chosen_db_str+'\n')
    wf.write('call_queuer_path\n')
    wf.write('python_path\n')
    wf.write('queuer_path\n')
    wf.write(email+'\n')
    wf.write(job_id+'\n')
    wf.write(classifier+'\n')
    wf.write(mutation_filename+'\n')
    wf.write(upload_filename+'\n')
    wf.write(user_upload_dir+'\n')
    wf.write(chosen_db_str+'\n')
    wf.write(tsv_report+'\n')
    wf.write(functional_annot+'\n')
    wf.write(hg18+'\n')
    wf.write(analysis_type+'\n')
    wf.write(str(no_input_line)+'\n')
    wf.write('error_output_path\n')
    wf.write(mupit_input+'\n')
    wf.close()
    r=ResQ()
    argstr  =       'dummy'
    argstr += ' ' + 'yes'
    argstr += ' ' + user_upload_dir
    argstr += ' ' + resubmit
    argstr += ' ' + job_id
    argstr += ' -e ' + email
    argstr += ' -i ' + job_id
    argstr += ' -c ' + classifier
    argstr += ' -m ' + mutation_filename
    argstr += ' -u ' + upload_filename
    argstr += ' -D ' + user_upload_dir
    argstr += ' -d ' + chosen_db_str
    argstr += ' -t ' + tsv_report
    argstr += ' -f ' + functional_annot
    argstr += ' -r ' + hg18
    argstr += ' -y ' + analysis_type
    argstr += ' -n ' + str(no_input_line)
    argstr += ' -M ' + mupit_input
    argstr += ' -R ' + resubmit
    argstr += ' -Q ' + queue_name
    argstr += ' -T ' + 'yes' # -T option for 'test'
    r.enqueue(queue_name, masteranalyzer.MasterAnalyzer, argstr)
    global job_ids
    job_ids.append(job_id)

##
## GENOMIC
##
#input_coordinate = 'genomic'
#for hg18_option in hg18_options:
#    for analysis_type in analysis_types:
#        for analysis_programs in analysis_programs_by_analysis_type[analysis_type]:
#            queue_job(input_coordinate=input_coordinate, \
#                      hg18_option=hg18_option, \
#                      analysis_type=analysis_type, \
#                      analysis_programs=analysis_programs)
##            sys.exit()
#queue_job(input_coordinate='genomic', \
#          hg18_option='off',
#          sample_id='no', \
#          analysis_type='driver', \
#          analysis_programs='CHASM')
#
##
## TRANSCRIPT
##
#input_coordinate = 'transcript'
#for analysis_type in analysis_types:
#    for analysis_programs in analysis_programs_by_analysis_type[analysis_type]:
#        for transcript_type in transcript_types:
#            if transcript_type in ['CCDS', 'ENST', 'ENSP']:
#                for refseq_correspondence in ['yes', 'no']:
#                    queue_job(input_coordinate=input_coordinate, \
#                              transcript_type=transcript_type, \
#                              refseq_correspondence=refseq_correspondence, \
#                              analysis_type=analysis_type, \
#                              analysis_programs=analysis_programs)
#            else:
#                queue_job(input_coordinate=input_coordinate, \
#                          transcript_type=transcript_type, \
#                          analysis_type=analysis_type, \
#                          analysis_programs=analysis_programs)
#queue_job(input_coordinate='transcript', \
#          hg18_option='off',
#          transcript_type='NM', \
#          sample_id='no', \
#          analysis_type='driver', \
#          analysis_programs='CHASM')
#
#wf = open('jobs_ids.txt', 'w')
#for job_id in job_ids:
#    wf.write(job_id + '\n')
#wf.close()

class CravatTest ():

    def __init__ (self, server='prod'):
        pass
    
    def run_test(self, \
                 input_coordinate='genomic',\
                 hg18_option='off', \
                 transcript_type='NA', \
                 refseq_correspondence='no', \
                 sample_id='yes',\
                 analysis_type=None, \
                 analysis_programs=None, \
                 input_basename=None):
        if input_basename == None:
            input_basename = input_coordinate + '_' + hg18_option + '_' + transcript_type + '_' + refseq_correspondence + '_' + sample_id + '.txt'
        else:
            hg18_option = input_basename.split('_')[1]
        input_filename = os.path.join(self.source_dir, input_basename)
        return job_id
    
    def quit (self):
        self.driver.quit()

def get_time_string ():
    [year, mon, day, hour, min, sec, wday, yday, isdst] = time.localtime()
    time_str = year + '_' + mon + '_' + day + '_' + hour + '_' + min + '_' + sec
    return time_str

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print 'Usage: python inside_server_submit_test.py server_type'
        print ''
        print '       server_type: dev, prod'
        sys.exit(-1)
    server_type = sys.argv[1]
    c = CravatTest(server=server_type)
    (source_dir, dummy) = os.path.split(os.path.abspath(__file__))
    print '__file__ =',__file__
    c.source_dir = source_dir
    print 'source_dir=',source_dir
    test_definition_filename = os.path.join(source_dir, 'test_definition.txt')
    log_filename = test_definition_filename + '.' + \
                   server_type + '.' + \
                   get_time_string() + '.log'
    f = open(test_definition_filename)
    wf = open(log_filename, 'w')
    for line in f:
        if line[0] != '#':
            toks = line.rstrip().split(' ')
            basename = toks[0]
            analysis_type = toks[1]
            print basename, ':', analysis_type
            job_id = c.run_test(input_basename=basename, analysis_type=analysis_type)
            if job_id != None:
                wf.write(basename + ' ' + analysis_type + ' ' + job_id + '\n')
                print basename, analysis_type, job_id
    f.close()
    wf.close()
    c.quit()