import sys
import os
import traceback
import re
import requests
import time
import json
import urllib
import shutil
import common_needed_functions


# action: 0
#     Parse the xena files. Make CRAVAT input and get CRAVAT output. Make db input files
# action: 1
#     Make folders for the TCGA.(tissue_specified).Xena.dat files and put the files in the folder
# action: 2
#     I don't rememmber but it looks for errors
# action: 3
#     Fills the specified database with the TCGA variants for the tissues
    

table_name = 'tcga_aa_variants'
table_fields = 'tissue, hugo, cravat_transcript, ref_aa, alt_aa, aa_pos, so, num_sample'
table_scheme = 'tissue varchar(10), hugo varchar(25), '+\
               'cravat_transcript varchar(45), ref_aa varchar(45), alt_aa varchar(45), '+\
               'aa_pos int(11), so varchar(2), num_sample INT' 

host_connections = {}
host_connections["local"] = {}
host_connections["local"]["mysql_host"] = 'localhost'
host_connections["local"]["mysql_user"] = 'root'
host_connections["local"]["mysql_password"] = '1234'
host_connections["local"]["db_name"] = 'annot_liftover'

host_connections["jhu"] = {}
host_connections["jhu"]["mysql_host"] = 'karchin-db01.icm.jhu.edu'
host_connections["jhu"]["mysql_user"] = 'rachelk'
host_connections["jhu"]["mysql_password"] = 'chrislowe'
host_connections["jhu"]["db_name"] = 'cravat_annot_dev'  



# CRAVAT input example
# ID    chromosome    genomic_pos    strand    ref_nuc    alt_nuc    sample(Dont need sample)
# tcga2    chr20    35128865    +    -    CTA    sample_4


# This is a global variable array that says which kind of tissues are in hg18 coordinates
# GLOBAL
# GLOBAL
# GLOBAL
tissues_in_hg18_coordinates = ['LAML']

# ==================================================================================================================================================
# ==================================================================================================================================================
# The section below is for submitting the cravat input file for a tissue into CRAVAT




def submit_cravat_job(dir_including_tissue, cravat_input_folder_name, cravat_output_dir, cravat_input_in_order, tissue_looking_at):

    success = 0
    cravat_output_files_in_order = []
    server_being_used = 'dev2'
    
    try:

#         Submit all the CRAVAT input files as separate jobs
        jobs_folder = os.path.join(dir_including_tissue, cravat_input_folder_name)
        jobs = os.listdir(jobs_folder)
        job_ids_array = []
        jobId_and_input_file_names_dict = {}

        print "\t\t\tSubmitting Jobs:"
        for job in jobs:
            # POST with an input file
            job_with_path = os.path.join(dir_including_tissue, cravat_input_folder_name, job)
            cravat_submission = None
            if tissue_looking_at in tissues_in_hg18_coordinates:
                print '\t\t\t\tUsing hg18 coordinates'
                cravat_submission = requests.post('http://'+server_being_used+'.cravat.us/rest/service/submit', \
                                files={'inputfile':open(job_with_path)}, \
                                data={'email':'dgygax@insilico.us.com','analyses':'', 'hg18':'on'})
            else:
                cravat_submission = requests.post('http://'+server_being_used+'.cravat.us/rest/service/submit', \
                                files={'inputfile':open(job_with_path)}, \
                                data={'email':'dgygax@insilico.us.com','analyses':'', 'hg18':'off'})
             
            cravat_info_dict = json.loads(cravat_submission.text)
            job_id = cravat_info_dict['jobid']
            jobId_and_input_file_names_dict[str(job_id)] = job
            print "\t\t\t\t" + str(job_id)
            job_ids_array.append(job_id)
            
        
#         Checking if the jobs have finished!!!
#         This will not exit until all the jobs are done!!!
        jobs_complete_array = []
        print '\t\t\tChecking Jobs:'
        while len(job_ids_array) > 0:
            for job_id_checking in job_ids_array:
                job_id_check_cravat = requests.get('http://'+server_being_used+'.cravat.us/rest/service/status?jobid='+job_id_checking)
                job_id_check_cravat_dict = json.loads(job_id_check_cravat.text)
                job_id_job_status = job_id_check_cravat_dict['status']    
                if job_id_job_status == 'Success' or job_id_job_status == 'Salvaged':
                    print "\t\t\t\t" + str(job_id_checking) + ' FINISHED!!!!'
                    jobs_complete_array.append(job_id_checking)
                    job_ids_array.remove(job_id_checking)
                elif job_id_job_status == "Error":
                    print '\t\t\t\tJob ' +str(job_id_checking)+ ' FAILED!!!'
                    print 'NOT ALL THE JOBS WHERE FINISHED!!!!'
                    return 0 
                
            if len(job_ids_array) == 0:
                break
            time.sleep(10)
        
#         Retrieve the completed jobs!!!
        print '\t\t\tRetrieving the completed Jobs:'
        prefixes_and_cravat_output_name = {}
        if not os.path.exists(os.path.join(dir_including_tissue, cravat_output_dir)):
            os.makedirs(os.path.join(dir_including_tissue, cravat_output_dir)) 
        for job_id_complete in jobs_complete_array:
#             downloading HAS to be dev
            result_URL = 'http://dev.cravat.us/results/'+job_id_complete+'/Variant_Additional_Details.Result.tsv'
            prefix_to_cravat_output_file_name = jobId_and_input_file_names_dict[str(job_id_complete)][:len(jobId_and_input_file_names_dict[str(job_id_complete)])-4]
            result_variantAdditionalDetails_filename =  os.path.join(dir_including_tissue, cravat_output_dir, prefix_to_cravat_output_file_name + '_Variant_Additional_Details.Result.tsv')
            prefixes_and_cravat_output_name[prefix_to_cravat_output_file_name] = prefix_to_cravat_output_file_name + '_Variant_Additional_Details.Result.tsv'
            print '\t\t\t\tjobId:' +str(job_id_complete)+ '  and prefix = ' + prefix_to_cravat_output_file_name
            print '\t\t\t\t\tjob.result_url = '  + str(result_URL)           
            urllib.urlretrieve(result_URL, result_variantAdditionalDetails_filename)
        
        
#         Saving the cravat output in the correct order
        for input_prefix in cravat_input_in_order:
            cravat_output_files_in_order.append(prefixes_and_cravat_output_name[input_prefix])
        
        success = 1
    except Exception, f:
        sys.stderr.write(str(repr(f)))
        sys.stderr.write(str(traceback.format_exc()))
       
    
    return cravat_output_files_in_order, success



# ==================================================================================================================================================
# ==================================================================================================================================================
# The section below is for reading the CRAVAT excel output of the tcga variants 


# ==================================================================================================================================================
# This function constructs a dictionary with the hugo symbol as the key and the corresponding CRAVAT transcript
# used in the interactive results webpage as the key
def construct_dict_hugo_key_cravat_transcript_value(dir, hugo_to_transcript_path):
    success = 0
    
    dict_hugo_to_transcript = {}
    dict_hugo_transcript_aaLen = {}
    
    try:
        rf = open(hugo_to_transcript_path, 'rU')
        wf = open(os.path.join(dir, "hugos_matching_more_than_one_transcript_IN_hugo_transcript.txt"), 'w')
        
        for line in rf:
            line = line.rstrip("\n\r")
            toks = line.split("\t")
            
            hugo = toks[8]
            cravat_transcript = toks[7]
            aa_len = int(toks[6])
            
            if hugo in dict_hugo_to_transcript:
               prev_aaLen_transcript = dict_hugo_transcript_aaLen[hugo]
               prev_aaLen = int(prev_aaLen_transcript.split("@@")[0])
               prev_trans = prev_aaLen_transcript.split("@@")[1]
                
               if prev_aaLen < aa_len:
                   wf.write("hugo Bigger = "+hugo + " \n")
                   wf.write("prev_transc = "+prev_trans + " and prev_aaLen = "+ str(prev_aaLen) + "\n")
                   wf.write("new transcript = " + str(cravat_transcript) + " and new aaLen = " + str(aa_len) + "\n")
                   wf.write("\n")
                    
                   dict_hugo_to_transcript[hugo] = cravat_transcript
                   dict_hugo_transcript_aaLen[hugo] = str(aa_len) + "@@"+cravat_transcript
               elif prev_aaLen == aa_len:
                   if cravat_transcript == prev_trans:
                       pass
                   else:
#                        sys.stderr.write("Cravat transcripts "+cravat_transcript+" and "+prev_trans+ "have the same length!! BAD!!! Hugo = " +hugo + "\n")
#                        This is weird but pass. So the original one will stay there. The other will be ignored
                       pass
#                        sys.stderr.write("What is going on with hugo = "+hugo+".\n\t")
#                        sys.stderr.write(cravat_transcript + '\n\t')
#                        sys.stderr.write(prev_trans + '\n')
                   
               else:
                   wf.write("******************************\n hugo SHORTER = "+hugo + " \n")
                   wf.write("prev_transc = "+str(dict_hugo_to_transcript[hugo]) + " and prev_aaLen = "+ str(prev_aaLen) + "\n")
                   wf.write("new transcript = " + str(cravat_transcript) + " and new aaLen = " + str(aa_len) + "\n")
                   wf.write("******************************\n\n")
                
            else:
               dict_hugo_to_transcript[hugo] = cravat_transcript
               dict_hugo_transcript_aaLen[hugo] = str(aa_len)+"@@"+cravat_transcript           
        
        wf.close()
        rf.close()
        success = 1
    except Exception, e:
        sys.stderr.write("Error in construct_dict_hugo_key_cravat_transcript_value.\n")
        sys.stderr.write(str(traceback.format_exc()))
        try:
            wf.close()
            rf.close()
        except Exception, f:
            pass
    return dict_hugo_to_transcript, success


# ==================================================================================================================================================
# This function reads the excel output from CRAVAT for the tcga variants and picks the variant corresponding to the cravat transcript
def read_CRAVAT_output_from_TCGA(dict_hugo_transcript, dir_tissue, cravat_additional_vaiant_details_file, tissue_abrev, name_database_input_file):
    success = 0
    try:
        wf = open(os.path.join(dir_tissue, tissue_abrev+"_"+name_database_input_file), 'w')
        
        rf = open(cravat_additional_vaiant_details_file, 'rU')
        
        var_and_sampleId = {}
        
        start_reading = False
        missing_hugos = {}
        num_cravat_output_no_conversion = 0
        for row in rf:
            row = row.rstrip("\n\r")
            if start_reading == False:
                start_reading = common_needed_functions.determine_if_start_reading_cravat_output_file(row, 'tsv')
                continue
            row = row.split("\t")
            
            cravat_id = row[1]
            chr = row[2]
            pos = row[3]
            strand = row[4]
            refBase = row[5]
            altBase = row[6]
            
            full_variant = str(chr) + "_" + str(pos) + "_" + str(strand) + "_" + str(refBase) + "_" + str(altBase)
            
            sampleId = row[7]
            hugo = row[8]
            AA_change_and_transcript = row[13]
            
            rep_AA_change, rep_transcript, missing_hugos = choose_AA_change_for_corresponding_transcript(dict_hugo_transcript, hugo, AA_change_and_transcript, missing_hugos, cravat_id)
#                 print rep_AA_change
            if rep_AA_change != None:
                ref_AA, aa_pos, alt_AA, seq_ont = pick_apart_rep_AA(rep_AA_change)
                line_for_db_input = []
                line_for_db_input.append(tissue_abrev)
                line_for_db_input.append(hugo)
                line_for_db_input.append(rep_transcript)
                line_for_db_input.append(ref_AA)
                line_for_db_input.append(alt_AA)
                line_for_db_input.append(aa_pos)
                line_for_db_input.append(seq_ont)  
                             
                
                if full_variant not in var_and_sampleId:
                    var_and_sampleId[full_variant] = {}
                    var_and_sampleId[full_variant]["db_input_start"] = line_for_db_input
                    var_and_sampleId[full_variant]["samples"] = {}
                    var_and_sampleId[full_variant]["samples"][sampleId] = True
                else:
                    if sampleId not in var_and_sampleId[full_variant]:
                        var_and_sampleId[full_variant]["db_input_start"] = line_for_db_input
                        var_and_sampleId[full_variant]["samples"][sampleId] = True
                    else:
                        sys.stderr.write("why does this occur twice:\n")
                        sys.stderr.write("\t"+full_variant+"\n")
                        sys.stderr.write("\t"+sampleId+"\n")    
                        sys.stderr.write("\n")                    
                
                
                
                
#                     success_writing = write_tcga_database_input_line(wf, line_for_db_input)
#                     if success_writing == 0:
#                         raise Exception
                
            else:
#                     sys.stderr.write("The tcga cravat_id " + str(cravat_id) + " could not be converted to a tcga position for the database.\n")
                num_cravat_output_no_conversion += 1
     
         
        count_samples_for_variant(var_and_sampleId, wf) 
#         print "number cravat output that couldn't be converted = " + str(num_cravat_output_no_conversion)
#         print "number missing_hugos = " + str(len(missing_hugos))
#         print "The following hugos couldn't be matched:"
#         for miss_hugo in missing_hugos:
#             print "\t" + str(miss_hugo)

        rf.close()
        wf.close()
        success = 1
    except Exception, e:
        try:
            rf.close()
            wf.close()
        except Exception, f:
            pass
        sys.stderr.write(str(e))
        sys.stderr.write(str(traceback.format_exc()))
        sys.exit(-1)
    
    
    
    return success



# ==================================================================================================================================================
# This function goes through the AA changes for each transcript for a variant and picks out the AA change corresponding to the transcript that matches
# the cravat transcript of interest
def choose_AA_change_for_corresponding_transcript(dict_hugo_transcript, hugo, AA_change_and_transcript, missing_hugos, cravat_id):
    
    rep_AA_change = None
    rep_transcript = None
    
    try:
        cravat_rep_transcript = None
        if hugo in dict_hugo_transcript:
            cravat_rep_transcript = dict_hugo_transcript[hugo]
        else:
            if hugo not in missing_hugos:
                missing_hugos[hugo] = True
                
        if cravat_rep_transcript != None:
            AA_changes_transcripts = AA_change_and_transcript.split(",")
            for AA_chng_trnscpt in AA_changes_transcripts:
                transcript =  AA_chng_trnscpt.split(":")[0]
                AA_chng = AA_chng_trnscpt.split(":")[1]
                
                if transcript[0] == "*":
                    transcript = transcript[1:]
                
                if transcript == cravat_rep_transcript:
                    rep_transcript = transcript
#                     You are only recording the first time this occurs. Not if it occurs again
                    if rep_AA_change == None:
                        rep_AA_change = AA_chng
                    else:
                        sys.stderr.write("\t\t\tAt cravat_id = "+str(cravat_id)+" why does the hugo " +hugo+ " and the transcript "+transcript+ " have more than one representative!!!\n")
                        pass
#                         raise Exception
            
    except Exception, e:
        sys.stderr.write("Error in choose_AA_change_for_corresponding_transcript\n")
        sys.stderr.write(str(e))
        sys.exit(-1)
    
    
    return rep_AA_change, rep_transcript, missing_hugos



# ==================================================================================================================================================
# This function picks apart the representative AA change into the (refAA) (AA position) and the (altAA)
def pick_apart_rep_AA(AA_change):
    aa_regEx = re.compile("(\D+)(\d+|NA)(\D+)\((\w\w)\)")
    
    ref_AA = None
    aa_pos = None
    alt_AA = None
    seq_ont = None
    
    try:
        aa_parse = aa_regEx.search(AA_change)
        if aa_parse:
            ref_AA = aa_parse.group(1)
            aa_pos = aa_parse.group(2)
            alt_AA = aa_parse.group(3)
            seq_ont = aa_parse.group(4)
            if aa_pos == "NA":
                print "\t\t\tThe aa_pos was NA: " + AA_change
    except Exception, e:
        sys.stderr.write("Problem occurred in pick_apart_rep_AA.\n")
        sys.stderr.write("The AA_change " ++ " couldn't be parsed into refAA, aaPos, altAA, and seqOnt.\n")
        sys.stderr.write(str(repr(e)))
        sys.exit(-1)
        
#     print str(ref_AA), str(aa_pos), str(alt_AA), str(seq_ont)
    
    
    return ref_AA, aa_pos, alt_AA, seq_ont




# ==================================================================================================================================================
# This function will count the number of samples that occur for a variant and then create an array for printing the tcga input for the variant
#        This variant line will have the sample number
# This is so we can keep track of how often a variant is seen in samples
def count_samples_for_variant(var_and_sampleId, wf):
    
    try:
        for variant in var_and_sampleId:
            dict_for_var =  var_and_sampleId[variant]
            
            db_input_start = dict_for_var["db_input_start"]
    #         print db_input_start
            samples_in_var = dict_for_var["samples"]
            
            num_samples = 0
            for sample in samples_in_var:
                num_samples += 1
                
            input_for_db = []
            for piece in db_input_start:
                input_for_db.append(piece)
            input_for_db.append(num_samples)
            
            write_tcga_database_input_line(wf, input_for_db)
    except Exception:
        sys.stderr.write(str(traceback.format_exc()))
        wf.close()
        sys.exit(-1)
        
    return



# ==================================================================================================================================================
# This function writes the lines for the input file the will be used to load the tcga database
# It will have the columns in order:
# tissue_type_abreviation    hugo    transcript  refAA   altAA   aaPos   SeqOnt    SampleNumber
def write_tcga_database_input_line(wf, line_for_db_input):
    
    success = 0
    
    try:
        for piece in line_for_db_input:
            wf.write(str(piece) + "\t")
        wf.write("\n")
    
        success = 1
    except Exception, e:
        sys.stderr.write("error occurred in write_tcga_database_input_line\n")
        sys.stderr.write(str(traceback.format_exc()))
        wf.close()
        sys.exit(-1)
    
    return success



# ==================================================================================================================================================
# ==================================================================================================================================================







def combine_all_cravat_output_files(tissue_path, cravat_output_folder, cravat_output_files_in_order, combined_cravatOutput_folder):

    success = 0
    combined_cravat_output_name = "CRAVAT_Output_Collective_Variants.tsv"
    try:
        collective_CRAVAT_Output_folder = os.path.join(tissue_path, combined_cravatOutput_folder)
        if not os.path.exists(collective_CRAVAT_Output_folder):
            os.makedirs(collective_CRAVAT_Output_folder) 
            
        output_file_number = 0
        combined_cravat_output_name_and_path = os.path.join(tissue_path, combined_cravatOutput_folder, combined_cravat_output_name)
        wf = open (combined_cravat_output_name_and_path, 'w')
        fail_while_combining_outputs = False
        for output_name in cravat_output_files_in_order:
            output_file_number += 1
            try:
                file_path = os.path.join(tissue_path, cravat_output_folder, output_name)
                rf = open(file_path, 'rU')
                start_reading_cravat_output_lines = False
                for line in rf:
                    line = line.rstrip("\n\r")
                    if start_reading_cravat_output_lines == False:
                        if output_file_number == 1:
                            wf.write(line + "\n")
                        start_reading_cravat_output_lines = common_needed_functions.determine_if_start_reading_cravat_output_file(line, 'tsv')
                        continue
                    else:
                        wf.write(line + "\n")
                rf.close()
            except Exception, badTry:
                fail_while_combining_outputs = True
                try:
                    rf.close()
                    print "Error on the inner try in combine_all_cravat_output_files()"
                    print repr(badTry)
                    print traceback.format_exc()
                except Exception, innerBadTry:
                    print "Error on the inner inner try in combine_all_cravat_output_files()"
                    print repr(innerBadTry)
                    print traceback.format_exc()
                break
        if fail_while_combining_outputs == True:
            raise ValueError("An error has occurred while trying to combine outputs in combine_all_cravat_output_files()\n\tDO NOT USE THE DB input for this tissue")
        wf.close()
        success = 1
    except Exception, e:
        try:
            wf.close()
        except Exception, feee:
            print feee
        print repr(e)
        print traceback.format_exc()
    
    return combined_cravat_output_name, success




# This function takes the xena file and parses throught it building a cravat input file
# When doing so it checks for duplicates and makes sure they are not looked at
# A duplicate means the same value occures for:
#     chromosome
#     start
#     strand
#     reference
#     alt
#     sample
# All of those things must be the same for a duplicate. Otherwise it does not count as a duplicate
# This is creating multiple CRAVAT input files.
#     Each one is only a limited size. This will allow many CRVAT jobs to run at once
#     You need to fix submit_cravat_job() so that this works right
def build_cravat_input_from_xena(tissue, tissue_folder, tissue_file_name):
    
    success = 0
    variants_detected = {}
    num_total_var_seen = 0
    num_variants = 0
    num_dups = 0
    cravat_input_folder = "Input_CRAVAT"
    cravat_id = 1
    array_cravat_input_files_in_order = []

    try:
        cravat_folder_for_inputs_path = os.path.join(tissue_folder, cravat_input_folder)
        if not os.path.exists(cravat_folder_for_inputs_path):
            os.makedirs(cravat_folder_for_inputs_path)
        
        tissue_file_name_and_path = os.path.join(tissue_folder, tissue_file_name)
        
        
        unique_var_when_counting = {}
        num_unique_var_in_xena_file = 0
        rf_count_lines = open(tissue_file_name_and_path, 'rU')
        titles_c = []
        read_titles_c = False
        for line_c in rf_count_lines:
            line_c = line_c.rstrip("\n\r")
            toks_c = line_c.split('\t')
            if read_titles_c == False:
                if toks_c[0][0] == '#':
                    titles_c = toks_c
                    titles_c[0] = titles_c[0][1:]
                    read_titles_c = True
                    continue
            line_c_dic = make_dictionary_of_titles_and_line_tabs(titles_c, toks_c)
            variant_c = line_c_dic["chr"] + "_" + line_c_dic["start"] + "_+_" + line_c_dic["reference"] + "_" + line_c_dic["alt"] + "_" + line_c_dic["sample"]
            if variant_c not in unique_var_when_counting:
                unique_var_when_counting[variant_c] = True
                num_unique_var_in_xena_file += 1
        rf_count_lines.close()
        divide_num_var_by_twenty = num_unique_var_in_xena_file/20
        
        
        
        rf = open(tissue_file_name_and_path, 'rU')
        
        print "\t\t\tFiles:"
        print "\t\t\t\t" + tissue_file_name
        
        num_wf = 0
        cravat_input_file_first = os.path.join(tissue_folder, cravat_input_folder, "CravatInput_"+tissue+"_"+str(num_wf)+".tsv")
        wf = open(cravat_input_file_first, 'w')
        array_cravat_input_files_in_order.append("CravatInput_"+tissue+"_"+str(num_wf))
    
        read_titles = False
        titles = []
        for line in rf:
            line = line.rstrip("\n\r")
            toks = line.split("\t")
            if read_titles == False:
                if toks[0][0] == "#":
                    titles = toks
                    titles[0] = titles[0][1:]
                    read_titles = True
                    continue
            else:
                line_dic = make_dictionary_of_titles_and_line_tabs(titles, toks)
                
                chr = line_dic["chr"]
                start = line_dic["start"]
                strand = "+"
                ref = line_dic["reference"]
                alt = line_dic["alt"]
                sample =line_dic["sample"]
                
                variant = chr+'_'+start+'_'+strand+'_'+ref+'_'+alt+'_'+sample
                
                if variant not in variants_detected:
                    variants_detected[variant] = True
                    cravat_input = tissue+"_"+str(cravat_id)+'\t'+chr+'\t'+start+'\t'+strand+'\t'+ref+'\t'+alt+'\t'+sample
                    cravat_id += 1
                    
                    if num_variants>0 and num_variants%divide_num_var_by_twenty == 0:
                        wf.close()
                        num_wf += 1
                        cravat_input_file_next = os.path.join(tissue_folder, cravat_input_folder, "CravatInput_"+tissue+"_"+str(num_wf)+".tsv")
                        wf = open(cravat_input_file_next, 'w')
                        array_cravat_input_files_in_order.append("CravatInput_"+tissue+"_"+str(num_wf))
                        
                    wf.write(cravat_input + "\n")
                    num_variants += 1
                else:
                    num_dups += 1
                num_total_var_seen += 1
                    
        
        
        rf.close()
        wf.close()
        print "\t\t\tTissue Stats:"
        print "\t\t\t\tTotal Variants Seen: " + str(num_total_var_seen)
        print "\t\t\t\tNumber Unique Variants: " + str(num_variants)
        print "\t\t\t\tNumber Duplicate Variants: " + str(num_dups)
        success = 1
    except Exception, e:
        sys.stderr.write("\n\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write(str(traceback.format_exc()))
        sys.stderr.write("\n\n")
    
    print '\t\t\tCRAVAT Input Files:'
    for input_file_in_order in array_cravat_input_files_in_order:
        print '\t\t\t\t' + str(input_file_in_order)
    
    return array_cravat_input_files_in_order, cravat_input_folder, success


# This function takes the titles of a file and makes a dictionary with the keys
# being the titles and the values being parts of the line. The parts and titles are separated the same way in the line
def make_dictionary_of_titles_and_line_tabs(titles, toks):
    line_dict = {}
    tok_num = 0
    for tok in toks:
        line_dict[titles[tok_num]] = tok
        tok_num += 1
    return line_dict




# This function is organizing all the steps that need to take place in order to create a tcga variants database input file for a tissue
# Organizes the following steps:
#     1. Builds a CRAVAT file fron the tcga Xena input for a tissue
#     2. Submits the CRAVAT file to CRAVAT and retrieves the results
#     3. Reads the CRAVAT results and make a database input file. Uses the hugo_transcripts.txt file in this step
def xena_organize_steps_for_a_tissue(dir, tissue):
    
    steps_completed = 0
    
    hugo_to_transcripts = "hugo_transcript.txt"        #from Rick
    suffix_name_database_input_file = "db_input_file.tsv"
    
    tissue_folder = os.path.join(dir, tissue)
    
#=====================================================================================================================================================================================================
# Step 1
# Parse through the TCGA.tissue.Xena.dat file and make a CRAVAT input file    

    print "\t\tStarted making CRVAT Input file"
    tissue_file_name = "TCGA."+tissue+".Xena.dat"
    cravat_input_files_in_order, cravat_input_folder_name, success_building = build_cravat_input_from_xena(tissue, tissue_folder, tissue_file_name)
    if success_building == 1:
        print "\t\tFinished making CRAVAT Input files"
        steps_completed += 1
    else:
        print "FAILED TO MAKE CRAVAT INPUT files"
        return steps_completed
#====================================================================================================================================================================================================
# Step 2
# Pass the created CRAVAT input file into CRAVAT
    print "\t\tSubmitting file "+cravat_input_folder_name+" to CRAVAT"
    CRAVAT_output_dir = "Output_CRAVAT"
    cravat_output_in_order, success_cravat = submit_cravat_job(tissue_folder, cravat_input_folder_name, CRAVAT_output_dir, cravat_input_files_in_order, tissue)
    if success_cravat == 1:
        print "\t\tRetrieved the CRAVAT output"
        steps_completed += 1
    else:
        print "\t\tFAILED during submitting to CRAVAT"
        return steps_completed

#====================================================================================================================================================================================================
# Step 3
# combine all the cravat output files into ONE combination file
    print '\t\tStarted combining the separate CRAVAT output files into one collective file.'
    combined_cravat_output_folder = 'Combined_CRAVAT_Output'
    combined_output, success_combining = combine_all_cravat_output_files(tissue_folder, CRAVAT_output_dir, cravat_output_in_order, combined_cravat_output_folder)
    if success_combining == 1:
        print '\t\tFinished combining the separate CRAVAT output files into one collective file.'
        steps_completed += 1
    if success_combining == 0:
        print "FAILED IN STEP 3 WHEN COMBINING"
        return steps_completed

#====================================================================================================================================================================================================
# Step 4
# Go through the CRAVAT output and make a database input file
    print "\t\tParsing CRAVAT output to make database input file"
    tissue_abrev = tissue
    
    cravat_additional_variant_details_file = os.path.join(tissue_folder, combined_cravat_output_folder, combined_output) 
     
    dict_hugo_transcript, success_constructing_dict = construct_dict_hugo_key_cravat_transcript_value(dir, os.path.join(dir, hugo_to_transcripts))  
    if success_constructing_dict == 0:
        sys.stderr.write("\t\t\tEXIT EXIT EXIT\n")
        sys.stderr.write("\t\t\tWhile attempting construct_dict_hugo_key_cravat_transcript_value() an error occurred and so no more analysis on this tissue was completed.\n\n")
        return steps_completed   #Again I don't like using return like this but I am. Find a better way!!!
    success_make_db_input = read_CRAVAT_output_from_TCGA(dict_hugo_transcript, tissue_folder, cravat_additional_variant_details_file, tissue_abrev, suffix_name_database_input_file)
      
    if success_make_db_input == 1:
        print "\t\tFinished making the database input file"
        steps_completed += 1
    else:
        print "FAILED when making the database input file"
        return steps_completed
        
    return steps_completed





# This function goes through the directory containing all the tissue and passes each tissue into xena_organize_steps_for_a_tissue
def loop_through_tcga_tissues_xena(dir):
    
    print "Making tcga input from Xena"
    for tissue in os.listdir(dir):
        if tissue == ".DS_Store" or tissue == "hugo_transcript.txt" or tissue == "hugos_matching_more_than_one_transcript_IN_hugo_transcript.txt":
            continue
        print "\tTissue: " + str(tissue)
        completed_steps = xena_organize_steps_for_a_tissue(dir, tissue)
        if completed_steps < 4:
            print '\tOnly made it through step ' + str(completed_steps)
            print '\tNOT ALL STEPS WERE COMPLETED FOR THIS TISSUE!'
            print '\t@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
        print
        print
    return



# This function will make a folder for each tissue
# These will be made and names based on the tissue files
#     The tissue files will be moved into the corresponding folders
def make_folders_for_tissues(dir):

    for tissue_file in os.listdir(dir):
#         If not a tissue data file then move to the next one
        if tissue_file[len(tissue_file)-4:] != ".dat":
            print "Passing file: " + str(tissue_file)
            continue
        else:
            tissue_abrv = tissue_file.split(".")[1]
            tissue_folder = os.path.join(dir, tissue_abrv)
            if not os.path.exists(tissue_folder):
                os.makedirs(tissue_folder)       
            
            shutil.move(os.path.join(dir,tissue_file), os.path.join(tissue_folder,tissue_file))

    return








# ===========================================================================================================================================================
# ===========================================================================================================================================================
# This section is for identifying the input errors for the job_ids from a list of cravat jobs.
# This is will be for errors that say wrong reference base input
def pick_out_job_ids(dir):
    import re
    input_file_regEx = re.compile('\d\.tsv')
    
    success = 0
    file_with_jobIds = 'JobIdList.tsv'
    input_file_prefix = 'CravatInput_'
    tissue_jobIds_dict = {}
    input_title_seen = {}
    try:
        file_and_path_for_jobIds = os.path.join(dir, file_with_jobIds)
        rf = open(file_and_path_for_jobIds, 'rU')
        for line in rf:
            line = line.rstrip('\r\n')
            if line == '':
                continue
            toks = line.split('\t')
            job_id = toks[0]
            input_title = toks[1]
            if input_file_regEx.match(input_title) == False:
                continue
            if input_title in input_title_seen:
                continue
            else:
                input_title_seen[input_title] = True
            if input_file_prefix not in input_title:
                continue
            input_name_pieces = input_title.split('_')
            tissue = input_name_pieces[1]
            if tissue not in tissue_jobIds_dict:
                tissue_jobIds_dict[tissue] = []
                tissue_jobIds_dict[tissue].append(job_id)
            else:
                tissue_jobIds_dict[tissue].append(job_id)
    

            
            
        rf.close()
        
        success = 1
    except Exception, e:
        print 'Failed to identify the jobIds associated with each tissue'
        print repr(e)
        print traceback.format_exc()
    
    return tissue_jobIds_dict, success


def retrieve_input_errors_files(tissues_jobIds_dict, inputErrorFolder):
    success = 0
    print '\t\tError folders in ' + inputErrorFolder
    try:
        print '\t\t\tTissues:'
        for tissue in tissues_jobIds_dict:
            tissues_errors_folder = os.path.join(inputErrorFolder, tissue)
            print '\t\t\t\t' + tissue
            if not os.path.exists(tissues_errors_folder):
                os.makedirs(tissues_errors_folder)
            num_err_files = 0
            for jobId in tissues_jobIds_dict[tissue]:
                num_err_files += 1
    #             downloading HAS to be dev
                input_error_URL = 'http://dev.cravat.us/results/'+jobId+'/Input_errors.Result.tsv'
                input_error_filename_path =  os.path.join(inputErrorFolder, tissue, jobId+ '_Input_errors.Result.tsv')         
                urllib.urlretrieve(input_error_URL, input_error_filename_path)    
            print '\t\t\t\t\t' + str(num_err_files) + ' files'
    
        success = 1
    except Exception, e:
        print repr(e)
        print traceback.format_exc()       
            
    return success



def go_through_input_errors(inputErrorFolder):
    
    success = 0
    
    list_tissues = os.listdir(inputErrorFolder)
    print '\t\tTissues:'
    for tissue in list_tissues:
        if tissue == ".DS_Store":
            continue
        print '\t\t\t' + tissue
        tissues_error_folder = os.path.join(inputErrorFolder, tissue)
        error_files = os.listdir(tissues_error_folder)
        tissue_contains_input_errors = False
        num_errors = 0
        for err_file in error_files:
            err_file_path = os.path.join(inputErrorFolder, tissue, err_file)
            rf = open(err_file_path, 'rU')
            start_reading = False
            for line in rf:
                line = line.rstrip('\r\n')
                toks = line.split("$%$")
                if start_reading == False:
                    if toks[0] == 'Input line number':
                        start_reading = True
                    continue
                else:
                    error_desc = toks[3]
                    if 'Input reference base' in error_desc:
                        num_errors += 1
#                         print line
#                         if tissue == 'PANCAN':
#                             print line
#                         if tissue_contains_input_errors == False:
#                             print '\t\t\t\t' + tissue + ' contains a referencew base input error'
#                             tissue_contains_input_errors = True
            
        print '\t\t\t\tNum Errors: ' + str(num_errors) 
    return



def organize_picking_out_input_errors(dir):
    
#     Step 1: Pick out the jobIds associated with tissues
    print '\tPicking out the JobIds'
    tissues_jobIds_dict, success_getting_jobIds = pick_out_job_ids(dir)
    if success_getting_jobIds == 0:
        print 'FAILED to get the JobIds'
        report_error_and_exit()
    print '\tFinished picking out JobIds'
    
#     Step 2: Getting the input error files for each tissue
    print '\tRetrieving the InputError Files:'
    inputErrorFolder = os.path.join(dir, 'InputErrors')
    if not os.path.exists(inputErrorFolder):
        os.makedirs(inputErrorFolder) 
    success_getting_error_files = retrieve_input_errors_files(tissues_jobIds_dict, inputErrorFolder)
    if success_getting_error_files == 0:
        print 'FAILED retrieving the error files'
        report_error_and_exit()
        
#     Step 3: Go through the error files to identify if there was a reference input error
    print '\tIdentifying Reference Base input errors'
    go_through_input_errors(inputErrorFolder)
    
    return


def report_error_and_exit():
    print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
    sys.exit(-1)
    return



# ===========================================================================================================================================================
# This section if for loading the database
def make_and_load_database(directory, sever):
     
     
    try:
        db = connect_to_database(sever)
        cursor = db.cursor() 

        stmt = 'drop table if exists '+table_name
        cursor.execute(stmt)
          
        stmt = 'create table '+table_name+' ('+table_scheme+') engine=innodb'
        cursor.execute(stmt)
        
        list_of_files = os.listdir(directory)
        for tissue_dir in list_of_files:
            if tissue_dir == '.DS_Store' or tissue_dir == 'hugo_transcript.txt' or tissue_dir == 'hugos_matching_more_than_one_transcript_IN_hugo_transcript.txt':
                continue
            input_file_name = tissue_dir + "_db_input_file.tsv"
            input_file = os.path.join(directory, tissue_dir, input_file_name)
            stmt = "load data local infile'"+input_file+"' into table "+table_name+" ("+table_fields+")"
            cursor.execute(stmt)
            
        db.commit()
        cursor.close()
        db.close()
    except Exception, e:
        sys.stderr.write(str(repr(e)))
        sys.stderr.write(str(traceback.format_exc()))
        try:
            cursor.close()
            db.close()
        except NameError, f:
            sys.stderr.write(str(repr(f)))
                 
    return




def connect_to_database(connection_type):
     
    try:
         
        db = MySQLdb.connect(host=host_connections[connection_type]["mysql_host"],\
                                 user=host_connections[connection_type]["mysql_user"],\
                                 passwd=host_connections[connection_type]["mysql_password"],\
                                 db=host_connections[connection_type]["db_name"])    
    except Exception, e:
        sys.stderr.write("Could not connect to the database " + str(host_connections[connection_type]["mysql_host"]) +".")
        sys.exit(-1)
     
    return db


if __name__ == "__main__":
    
# There is one global variable above that defines which tissues are in hg18 coordinates.
# The variable is called:
#         tissues_in_hg18_coordinates
# YOU MUST MAKE SURE THIS IS CORRECT!!!!!
    action = int(sys.argv[1])
    source = sys.argv[2]
    dir = sys.argv[3]
    server = sys.argv[4]
    
    if action == 0:
        if (source == "xena"):
            loop_through_tcga_tissues_xena(dir)
        else:
            orchestrate_pasrsing_many_tissues()
    elif action == 1:
        make_folders_for_tissues(dir)
    elif action == 2:
        organize_picking_out_input_errors(dir)
    elif action == 3:
        make_and_load_database(dir, server)
