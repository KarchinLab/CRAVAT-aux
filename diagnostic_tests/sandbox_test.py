import os
import shutil
import subprocess
import sys
import time

class CravatSandboxTest ():

    def __init__ (self):
        pass
    
    def run_test(self, \
                 input_coordinate='genomic',\
                 hg18_option='off', \
                 transcript_type='NA', \
                 refseq_correspondence='no', \
                 sample_id='yes',\
                 job_id=None, \
                 input_filename=None, \
                 analysis_type=None, \
                 ):
        if os.path.exists(input_filename) == False:
            print 'Error: '+input_filename+' does not exist. Skipping this job'
            return False
        file_dir = os.path.dirname(os.path.abspath(__file__))
        cravat_result_dir = os.path.join(file_dir, '..', 'CRAVAT_RESULT', job_id)
        wrappers_dir = os.path.join(file_dir, '..', 'WebContent', 'Wrappers')
        if os.path.exists(cravat_result_dir):
            shutil.rmtree(cravat_result_dir)
        os.mkdir(cravat_result_dir)
        input_basename = os.path.basename(input_filename)
        subprocess.call(['cp', input_filename, cravat_result_dir])
        classifier = 'Breast'
        functional_annotation = 'on'
        if analysis_type == 'driver':
            analysis_program_str = 'CHASM_SnvGet'
        elif analysis_type == 'functional':
            analysis_program_str = 'VEST_SnvGet'
        elif analysis_type == 'geneannotationonly':
            analysis_program_str = 'None'
        cwd = os.getcwd()
        os.chdir(wrappers_dir)
        ret = subprocess.call(['bash', \
                               'run_cravat.sh', \
                               '-i', job_id, \
                               '-u', input_basename, \
                               '-y', analysis_type, \
                               '-d', analysis_program_str, \
                               '-c', classifier, \
                               '-r', hg18_option])
        os.chdir(cwd)
        if ret == 0:
            return True
        else:
            return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print 'Usage: python sandbox_test.py test_definition_filename log_filename'
        print ''
        print 'test_definition_filename is the filename of a test definition file.'
        print 'Each line of a test definition file has the following format:'
        print ''
        print '    job_id<SPACE>input_filename<SPACE>analysis_type<SPACE>(optional)hg18_switch'
        print ''
        print '    job_id is a CRAVAT analysis job ID and is a string of alphanumeric'
        print '    characters as well as dots and underscores. job_id will be used as' 
        print '    the name of the directory for the job which will be created under' 
        print '    CRAVAT_SANDBOX/CRAVAT_RESULT.'
        print '    For example, if a job\'s ID is driver_test, a folder' 
        print '    CRAVAT_SANDBOX/CRAVAT_RESULT/driver_test will be created and'
        print '    the CRAVAT analysis result for the job will be stored in this folder.'
        print ''
        print '    input_filename is the path of the input file for the job. The path can be'
        print '    either absolute or relative (to CRAVAT_SANDBOX/diagnostic_tests).'
        print '    The format of the input file is explained at' 
        print '    http://www.cravat.us/help.jsp?chapter=how_to_use&article=input.'
        print ''
        print '    analysis_type is either "driver", "functional", or "geneannotationonly",'
        print '    without quotation marks. If "driver" is given, CHASM and SnvGet analyses'
        print '    will be performed. If "functional" is given, VEST and SnvGet analyses'
        print '    will be performed. If "geneannotationonly" is given, only gene and mutation'
        print '    annotation will be performed.'
        print ''
        print '    hg18_switch is optional. For transcript-coordinate- or' 
        print '    hg19-genomic-coordinate-input files, hg18_switch is not needed.' 
        print '    For hg18-genomic coordinate input files, put "hg18" without quotation' 
        print '    marks after analysis_type and a space.'
        print ''
        print 'A file with the filename given as sandbox_run_log_filename' 
        print 'will be created in CRAVAT_SANDBOX/diagnostics with'
        print 'some log information from sandbox_test.py.'
        sys.exit(1)
    test_definition_filename = sys.argv[1]
    log_filename = sys.argv[2]
    c = CravatSandboxTest()
    (source_dir, dummy) = os.path.split(os.path.abspath(__file__))
    c.source_dir = source_dir
    f = open(os.path.join(source_dir, test_definition_filename))
    wf = open(os.path.join(source_dir, log_filename), 'w')
    for line in f:
        if line[0] != '#':
            toks = line.rstrip().split(' ')
            if len(toks) < 3:
                print 'Error: Each line of test_definition file should '+\
                      'have job ID, input file\'s basename, analysis '+\
                      'type, and optionally hg18 coordinate switch '+\
                      '(hg18/off)'
                print 'Offending line=['+line.rstrip()+']'
                sys.exit(-1)
            job_id = toks[0]
            filename = toks[1]
            analysis_type = toks[2]
            if len(toks) == 4:
                if toks[3] == 'hg18':
                    hg18 = 'on'
                else:
                    hg18 = 'off'
            else:
                hg18 = 'off'
            print '...', job_id, filename, analysis_type
            basename = os.path.basename(filename)
            ret = c.run_test(job_id=job_id, input_filename=filename, analysis_type=analysis_type, hg18_option=hg18)
            if ret == True:
                print '    Done'
                wf.write(filename + ' ' + analysis_type + ' ' + job_id + ' Done\n')
            else:
                print '    Error with the job', job_id
                wf.write(filename + ' ' + analysis_type + ' ' + job_id + ' Error\n')
    f.close()
    wf.close()
