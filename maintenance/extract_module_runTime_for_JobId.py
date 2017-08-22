import sys
import os
import common_needed_functions

# This script will extract the run time for each module for a set of jobIds 


jobIds_examining = {
                    'karchin' : {
                                 'all': ['dgygax_20150901_155555',
                                          'dgygax_20150901_155620',
                                          'dgygax_20150901_155742',
                                          'dgygax_20150901_155801',
                                          'dgygax_20150828_142319',
                                          'dgygax_20150828_142330',
                                          'dgygax_20150828_142342',
                                          'dgygax_20150828_142354',
                                          'dgygax_20150828_142406',
                                          'dgygax_20150828_142419',
                                          'dgygax_20150828_161654'
                                          ],
                                  'CHASM':['dgygax_20150902_095623',
                                            'dgygax_20150902_095556',
                                            'dgygax_20150902_095520',
                                            'dgygax_20150902_095453',
                                            'dgygax_20150902_095437',
                                            'dgygax_20150902_095421',
                                            'dgygax_20150902_094545',
                                            'dgygax_20150902_094524',
                                            'dgygax_20150902_094306',
                                            'dgygax_20150902_094247',
                                            'dgygax_20150901_180103'                      
                                            ],
                                  'VEST':['dgygax_20150903_095615',
                                        'dgygax_20150903_095559',
                                        'dgygax_20150903_095537',
                                        'dgygax_20150903_095511',
                                        'dgygax_20150903_095426',
                                        'dgygax_20150902_095805',
                                        'dgygax_20150902_095747',
                                        'dgygax_20150902_095732',
                                        'dgygax_20150902_095714',
                                        'dgygax_20150902_095700',
                                        'dgygax_20150902_095648'
                                          ]
                                 },
                     'local' : {
                                'all':['dgygax_20150828_142342', 
                                       'dgygax_20150901_155801'
                                       ],
                                'CHASM':[],
                                'VEST:':[]
                                }
                    }



all_module_names = [
            '1000 Genomes',
            'CHASM',
            'COSMIC',
            'DBResult',
            'ESP6500',
            'ExAC',
            'Gene Annotation',
            'Gene Level Score',
            'Information',
            'Mappability',
            'MuPIT',
            'Reporter',
            'SNVBox',
            'Statistics',
            'TARGET',
            'VCF',
            'VEST_FV',
            'VEST_IV',
            'VEST_MS',
            'VEST_SG',
            'VEST_SL',
            'VEST_SS',
            'Vogelstein',
            'dbSNP'
            ]

# This function loops through all the JobIds and individually sends each jobId to recover the number of total variants, number of coding variants, and the amount of time it took for each module to run
def organize_jobIds_searching(dir_read, dir_write, jobIds, analysis_type):
    
    wf = open(os.path.join(dir_write, 'timing_output_'+analysis_type+'_Analysis.txt'), 'w')
    
    for jobId in jobIds:
        wf.write('jobId\t' + str(jobId) + '\n')
        total_num_variants = get_total_num_variants(dir_read, jobId)
        wf.write('total num variants\t' + str(total_num_variants) + '\n')
        num_coding_variants = get_num_coding_variants(dir_read, jobId)
        wf.write('num coding variants\t' + str(num_coding_variants) + '\n')
        module_runtimes = read_progress_file(dir_read, jobId)
        
        for module in all_module_names:
            runtime_of_module = None
            if module in module_runtimes:
                runtime_of_module = str(module_runtimes[module])
            else:
                runtime_of_module = '0'
            wf.write(module + '\t' + runtime_of_module + '\n')
            
        wf.write('\n\n\n')
    
    wf.close()
    
    return


# This function retrieves the total number of variants input into a job
# It does so looking at the first line of the file 'job_info.txt' in the JobId directory
def get_total_num_variants(dir_read, jobId):
    file_name = 'job_info.txt'
    file_path = os.path.join(dir_read, jobId, file_name)
    rf = open(file_path, 'rU')
    line_num = 0
    total_num_variants = None
    for line in rf:
        line_num += 1
        line = common_needed_functions.remove_new_line_character(line)
        total_num_variants = line
        if line_num == 1:
            break
    rf.close()

    return total_num_variants


# This function retrieves the number of coding variants for a job
# It does so by reading the number of variants in the file 'Variant.Result.tsv' in the JobId directory
def get_num_coding_variants(dir_read, jobId):
    file_name = 'Variant.Result.tsv'
    file_path = os.path.join(dir_read, jobId, file_name)
    rf = open(file_path, 'rU')
    start_reading = False
    num_coding_variants = 0
    for line in rf:
        line = common_needed_functions.remove_new_line_character(line)
        if start_reading == False:
            start_reading = common_needed_functions.determine_if_start_reading_cravat_output_file(line, 'tsv')
            continue
        else:
            num_coding_variants += 1
            
    return num_coding_variants



# Read the progress file and get the length of time for each module
# Each new module starts with:
#      '>'
def read_progress_file(dir_read, jobId):
    progress_file_name = 'progress.txt'
    progress_file_path = os.path.join(dir_read, jobId, progress_file_name)
    
    f = open(progress_file_path, 'rU')
    module_runtimes = {}
    current_module = ''
    for line in f:
        if line[0] == '#':
            continue
        line = common_needed_functions.remove_new_line_character(line)
        toks = line.split('\t')
        if toks[0] == '>':
            [dummy, time, module] = toks
            
            if current_module != '':
                if current_module == 'VEST_MS' or 'VEST' not in current_module or ('VEST' in current_module and repeat_no == '9'):
                    module_runtimes[current_module] = int(float(time) - module_runtimes[current_module])
            
            if module == 'VEST':
                module = 'VEST_MS'
                module_runtimes[module] = float(time)
                current_module = module
            elif module == 'VEST IV':
                module = 'VEST_IV'
                module_runtimes[module] = float(time)
                current_module = module
            elif 'VEST' in module:
                [dummy, so, repeat_no, dummy, dummy] = module.split(' ')
                module = 'VEST_' + so
                if repeat_no == '0':
                    module_runtimes[module] = float(time)
                    current_module = module
            elif module != 'MasterAnalyzer':
                module_runtimes[module] = float(time)
                current_module = module
        else:
            if len(toks) == 3:
                [time, progress, msg] = toks
                if progress == '100' and msg == 'Finished' and 'VEST' not in current_module:
                    module_runtimes[current_module] = int(round(float(time) - module_runtimes[current_module]))
                    current_module = ''
    f.close()
    
    return module_runtimes



if __name__ == '__main__':
    server = sys.argv[1]
    user = sys.argv[2]
    analysis_type = sys.argv[3]
    dir_writing = sys.argv[4]
    dir_reading = None
    jobIds = jobIds_examining[server][analysis_type]
    if server == 'local':
        if user == 'derek':
            dir_reading = '/Users/derekgygax/Desktop/CRAVAT_Info_Test/timing_per_module/jobIds_extractTimingTester'
        elif user == 'rick':
            dir_reading = ''
    elif server == 'karchin':
        if user == 'stg':
            dir_reading = '/export/karchin-web02/CRAVAT_STG/tomcat/webapps/CRAVAT_RESULT'
        elif user == 'dev':
            dir_reading = '/export/karchin-web02/CRAVAT_DEV/tomcat/webapps/CRAVAT_RESULT'
    
    organize_jobIds_searching(dir_reading, dir_writing, jobIds, analysis_type)


