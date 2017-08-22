
import traceback
import sys

mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '50Y4xj6p'
db_name = 'uniprot_cravat_auto'
 
 
convertion_types_desired = {
                            "RefSeq_NT" : True,
                            "Ensembl_TRS" : True,
                            "CCDS" : True
                            }
array_convention_types_desired_in_order = ["RefSeq_NT", "Ensembl_TRS", "CCDS"]

table_name = 'uniprot_cravat_conversion'
table_fields = 'chrom, feature_key, description, start, stop, uniprot_id, aa_len, cravat_transcript, hugo'
table_scheme = 'chrom varchar(100), feature_key varchar(400), '+\
               'description varchar(400), start int(10), stop int(10), '+\
               'uniprot_id varchar(15), aa_len int(15), cravat_transcript varchar(25), hugo varchar(25)' 

# Examine a file created by the UCSC genome browser that shows uniProtIds
# This file originally has many replicates of the same uniProtId so go through it 
# and pick out all the distinct Ids
# Used then to find the convertion IDs from uniProt to other databases
# and
# get the AA length of the uniProt AA sequence
def retrieve_uniProtIds(rf_path, wf_path):
    
    uniprot_ids = {}
    
    try:
        
        rf = open(rf_path, 'rU')
        title_line_hit = 0
        tok_titles = []
        for line in rf:
            line = strip_new_line_characters(line)
            toks = split_by_tab(line)
            if title_line_hit == 0:
                tok_titles = toks
                tok_titles[0] = tok_titles[0][1:]     #This removes the # symbol from the front
                title_line_hit = 1
                continue
            toks_object = {}
            tok_num = 0
            for tok in toks:
                toks_object[tok_titles[tok_num]] = tok
                tok_num += 1
             
            if toks_object["uniProtId"] in uniprot_ids:
                pass
            else:
                toks_object["uniProtId"] = remove_leading_trailer_white_space(toks_object["uniProtId"])
                uniprot_ids[toks_object["uniProtId"]] = True
                
        rf.close()
        
#         wf = open(wf_path, 'w')
#         
#         for key in uniprot_ids:
#             wf.write(key)
#             wf.write("\n")
            
#         wf.close()
    except Exception, e:
        sys.stderr.write("Error in retrieve_uniProtIds()\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")
        try:
            rf.close()
#             wf.close()
        except Exception, f:
            sys.stderr.write("Cannot close the read and write files\n\n")
        sys.exit(-1)
    
    return uniprot_ids

# This function organizes the action of most of this file:
#     In order it does the following:
#         -Makes a dictionary of the convertions from uniprot IDs to the IDs in other databases
#         -Make a dictionary of the uniprot IDs and the lenght of the amino acid sequence for that ID
#         -Examine the refSeq, ENSEMBL, and CCDS transcripts in CRAVAT and find which ones have the same AA length as the transcript in the uniProt database
#         -Make an end table built from the uniProt table initially read that includes the uniProt AA length, and the covertion ID from uniProt to the
#             correct transcript in CRAVAT
def construct_full_uniprot_table(ucsc_table, uniprot_to_other_databases_convertion_file, uniprot_fasta, uniprot_full_table, dict_unique_uniProtId, cravat_transcripts_file):
    
    sent_by_raise = 0
    
    try:
        succss_making_uniprot_refSeq_dict, dict_uniprot_to_Other_database_Ids = make_dict_uniprot_to_otherDatabases(uniprot_to_other_databases_convertion_file, dict_unique_uniProtId) 
        if succss_making_uniprot_refSeq_dict == 0:
            sent_by_raise = 1
            raise Exception
         
        sucess_getting_AA_length, dict_uniprotID_AA_length = make_dict_uniprotID_AA_seq_length(uniprot_fasta, dict_uniprot_to_Other_database_Ids)
        if sucess_getting_AA_length == 0:
            sent_by_raise = 1
            raise Exception   
                
        success_make_CRAVAT_transcript_dicts, dict_CRAVAT_refSeq_AALength, dict_CRAVAT_ENSEMBL_AALength, dict_CRAVAT_CCDS_AALength = make_dicts_CRAVAT_transcripts_AA_length(cravat_transcripts_file, 1)
        if success_make_CRAVAT_transcript_dicts == 0:
            sent_by_raise = 1
            raise Exception  
                       
        success_getting_CRAVAT_transcript, dict_uniProtId_bestConvertionId = get_CRAVAT_transcript_with_same_AA_length_to_uniProt_convertion_transcripts(dict_uniprotID_AA_length, dict_uniprot_to_Other_database_Ids, dict_CRAVAT_refSeq_AALength, dict_CRAVAT_ENSEMBL_AALength, dict_CRAVAT_CCDS_AALength)
        if success_getting_CRAVAT_transcript == 0:
            sent_by_raise = 1
            raise Exception            
        
        success_writing_full_table = write_full_uniprot_table(ucsc_table, uniprot_full_table, dict_uniProtId_bestConvertionId, dict_uniprotID_AA_length)
        if success_writing_full_table == 0:
            sent_by_raise = 1
            raise Exception
        
        
    except Exception, e:
        if sent_by_raise == 1:
            pass
        else:
            sys.stderr.write("An error occurred in construct_full_uniprot_table()\n")
            sys.stderr.write(str(repr(e)) + "\n")
            sys.stderr.write(traceback.format_exc())
    return
  
  
# Goes through the file 'HUMAN_9606_idmapping.dat' and identifies the convertions from uniProt_Ids
# to the ID's in other databases
# Used to compare the uniProt transcript to the CRAVAT transcript
def make_dict_uniprot_to_otherDatabases(rf_path, dict_desired_uniProtId):
    success = 0
    dict_uniprot_convertion_Ids = {}
  
    try:
        rf = open(rf_path, 'rU')
        for line in rf:
            line = strip_new_line_characters(line)
            toks = split_by_tab(line)
            
#             Sometimes in the file the uniprotId has unprot-#. We don't know why but we are combining them all together so there are no hyphens
            array_unProt_id_around_hyphen = toks[0].split("-")
            uniProt_id = remove_leading_trailer_white_space(array_unProt_id_around_hyphen[0])
            convertion_type = remove_leading_trailer_white_space(toks[1])
            convertion_ID = remove_leading_trailer_white_space(toks[2])
            
            if uniProt_id in dict_desired_uniProtId:
                if convertion_type in convertion_types_desired:
                    if uniProt_id in dict_uniprot_convertion_Ids:
                        if convertion_type in dict_uniprot_convertion_Ids[uniProt_id]:                        
                            convertion_already_in = dict_uniprot_convertion_Ids[uniProt_id][convertion_type]
                            extended_convertion = convertion_already_in + "," + convertion_ID
                            dict_uniprot_convertion_Ids[uniProt_id][convertion_type] = extended_convertion
                        else:
                            dict_uniprot_convertion_Ids[uniProt_id][convertion_type] = convertion_ID 
                    else:
                        dict_uniprot_convertion_Ids[uniProt_id] = {}
                        dict_uniprot_convertion_Ids[uniProt_id][convertion_type] = convertion_ID    

                        
        # WHY ARE THERE SO MANY num_uniProtid_convertions_desired_but_no_have
        num_uniProtid_convertions_desired_but_no_have = 0
        for ID in dict_desired_uniProtId:
            if ID in dict_uniprot_convertion_Ids:
                pass
            else:
                num_uniProtid_convertions_desired_but_no_have += 1
                
        print "num_uniProtid_convertions_desired_but_no_have = " + str(num_uniProtid_convertions_desired_but_no_have)
                          
        rf.close()       
        success = 1
    except Exception, e:
        sys.stderr.write("An error occurred while in make_dict_uniprot_to_otherDatabases().\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")
        try:
            rf.close()
        except Exception, f:
            sys.stderr.write("An error occurred while in make_dict_uniprot_to_otherDatabases().\n")  
            sys.stderr.write("Cannot close read file")            
    
    return success, dict_uniprot_convertion_Ids

# Reads the file 'uniprot_sprot.fasta'
# Makes a dictionary of the uniProt_Ids and the AA length of their repsective transcripts
# Used to compare the length of the uniProt transcript to the length of convertion transcripts in CRAVAT
def make_dict_uniprotID_AA_seq_length(uniprot_fasta, dict_uniprot_to_Other_Database_Ids):

    success = 0
    exception_raise = 0
    dict_uniprotId_AA_length = {}
    try:
        rf = open(uniprot_fasta, 'rU')
        uniprot_id_examining = None
        AA_seq_examining = None
        for line in rf:
            line = strip_new_line_characters(line)
            if line[0] == ">":
                if AA_seq_examining != None:
                    AA_seq_examining = remove_leading_trailer_white_space(AA_seq_examining)
                    success_getting_AA_length, AA_length = count_AA_seq_length(AA_seq_examining)
                    if success_getting_AA_length == 0:
                        exception_raise = 1
                        raise Exception
                    if uniprot_id_examining in dict_uniprotId_AA_length:
                        sys.stderr.write("The uniprotId = "+uniprot_id_examining+" exists in the fasta file more than once!!!!\nThe program was quit and did NOT complete.\n")
                        sys.exit(-1)
                    else:
                        if uniprot_id_examining in dict_uniprot_to_Other_Database_Ids:
                            dict_uniprotId_AA_length[uniprot_id_examining] = AA_length
                        else:
                            sys.stderr.write("Somehow you hit a uniprotID that is not in dict_uniprot_to_Other_Database_Ids. You should not be able to do this. The uniprotID = '"+uniprot_id_examining+"'.\n\n")
                            sys.exit(-1)
                    
                AA_seq_examining = None
                uniprot_id_examining = None
                
#                 Get the uniprot id from the line
                success_retrieving_uniprot_id, uniprot_id_examining = retrieve_uniprot_id_from_fasta_line(line)
                uniprot_id_examining = remove_leading_trailer_white_space(uniprot_id_examining)
                if success_retrieving_uniprot_id == 0:
                    exception_raise = 1
                    raise Exception
                
            else:
                if uniprot_id_examining in dict_uniprot_to_Other_Database_Ids:
#                     If the uniprot_id_examining is in the dictionary of the uniprot_ids you are looking for then record the AA sequence for that uniprotID
                    line = remove_leading_trailer_white_space(line)
                    if AA_seq_examining == None:
                        AA_seq_examining = ""
                    AA_seq_examining = AA_seq_examining + line
                else:
#                     If the uniprot_id from the fasta file is not one of the uniprot_id's you are looking for then skip the line with the sequence for that uniprot id
#                     In doing so AA_seq_examining will remain None. So in the if statement for the ">" above you won't count the AA length or even look at dict_uniprotId_AA_length
                    continue
                
                
#         Need to look at the very last uniProtID in the file.
#         The way the loop goes the last uniprotId and sequence are NOT examined. So doing it here
        if uniprot_id_examining in dict_uniprot_to_Other_Database_Ids:
            if uniprot_id_examining in dict_uniprotId_AA_length:
                sys.stderr.write("The uniprotId = "+uniprot_id_examining+" exists in the fasta file more than once!!!!\nThe program was quit and did NOT complete.\n")
            else:
                AA_seq_examining = remove_leading_trailer_white_space(AA_seq_examining)
                success_getting_AA_length_var_after_Loop, AA_length_var_after_Loop = count_AA_seq_length(AA_seq_examining)
                if success_getting_AA_length_var_after_Loop == 0:
                    exception_raise = 1
                    raise Exception
                #If all of these first parts are passed then put the AA_length for the uniprotID in the dictionary
                dict_uniprotId_AA_length[uniprot_id_examining] = AA_length_var_after_Loop
        
        rf.close()        
        success = 1
    except Exception, e:
        if exception_raise == 1:
            pass
        else:
            sys.stderr.write("An error occurred while in make_dict_uniprotID_AA_seq_length().\n")
            sys.stderr.write(str(repr(e)))
            sys.stderr.write("\n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("\n")  
        try:
            rf.close()      
        except Exception, f:
            sys.stderr.write("An error occurred while in make_dict_uniprotID_AA_seq_length().\n")  
            sys.stderr.write("Cannot close read file")  
            
            
    return success, dict_uniprotId_AA_length

# When reading the uniProt fasta file it picks out the uniProt ID from the title line for each transcript
def retrieve_uniprot_id_from_fasta_line(fasta_line):
    success = 0
    uniprot_id = None
    try:
        toks = fasta_line.split("|")
        uniprot_id = toks[1]
        success = 1
    except Exception, e:
        sys.stderr.write("An error occurred while in retrieve_uniprot_id_from_fasta_line() when fasta_line = '"+fasta_line+"'.\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")         

    return success, uniprot_id


# Looks at the transcripts retrieved from the uniprot fasta file and gets their length
def count_AA_seq_length(AA_seq):
    length_AA_seq = None
    success = 0
    try:
        length_AA_seq = len(AA_seq)
        success = 1
    except Exception, e:
        sys.stderr.write("An error occurred while in count_AA_seq_length() when the AA_seq was = '"+AA_seq+"'\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")      
    return success, length_AA_seq




# This function makes three dictionary:
#     1. The CRAVAT refSeq transcript Id and it's amino acid length
#     2. The CRAVAT ENSEMBL transcript Id and it's amino acid length
#     3. The CRAVAT CCDS transcript Id and it's amino acid length
# These are used to determine if the uniProt transcript length is the same as respective refSeq, ENSEMBL, or CCDS transcript in that order upon convertion
def make_dicts_CRAVAT_transcripts_AA_length(CRAVAT_transcripts_file, why):
    
    success = 0
    dict_refSeq_AALength = {}
    dict_ENSEMBL_AALength = {}
    dict_CCDS_AALength = {}
    raised_exception = 0
    
    try:
        rf = open(CRAVAT_transcripts_file, 'rU')
        
        column_titles = ["UID", "CCDS", "RefseqT", "RefseqP", "EnsT", "EnsP", "protein_length"]
        
        for line in rf:
            line = strip_new_line_characters(line)
            toks = split_by_tab(line)
            
            toks_object = {}
            tok_num = 0
            for tok in toks:
                toks_object[column_titles[tok_num]] = tok
                tok_num += 1
            
            refSeq_transcript_id = toks_object["RefseqT"]
            refSeq_transcript_id = remove_leading_trailer_white_space(refSeq_transcript_id)
            
            ensembl_transcript_id = toks_object["EnsT"]
            ensembl_transcript_id = remove_leading_trailer_white_space(ensembl_transcript_id)
            
            ccds_transcript_id = toks_object["CCDS"]
            ccds_transcript_id = remove_leading_trailer_white_space(ccds_transcript_id)
            
            AA_length = None
            if why == 1:
                AA_length = toks_object["protein_length"]
            elif why == 2:
                AA_length = toks_object["UID"]
            
            success_refSeq, dict_refSeq_AALength = place_transcript_and_AA_length_in_dictionary(refSeq_transcript_id, AA_length, dict_refSeq_AALength, "refSeq")
            if success_refSeq == 0:
                raised_exception = 1
                raise Exception
            success_ENSEMBL, dict_ENSEMBL_AALength = place_transcript_and_AA_length_in_dictionary(ensembl_transcript_id, AA_length, dict_ENSEMBL_AALength, "ENSEMBL")
            if success_ENSEMBL == 0:
                raised_exception = 1
                raise Exception
            success_CCDS, dict_CCDS_AALength = place_transcript_and_AA_length_in_dictionary(ccds_transcript_id, AA_length, dict_CCDS_AALength, "CCDS")
            if success_CCDS == 0:
                raised_exception = 1
                raise Exception            
        
        rf.close()
        success = 1
    except Exception, e:
        if raised_exception == 1:
            pass
        else:
            sys.stderr.write("An error occurred while in make_dicts_CRAVAT_transcripts_AA_length()\n")
            sys.stderr.write(str(repr(e)))
            sys.stderr.write("\n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("\n")       
        try:
            rf.close()
        except Exception, f:
            sys.stderr.write("The read and write files could not be closed in write_full_uniprot_table()\n")
            sys.stderr.write(repr(f))
            sys.stderr.write("\n\n")              
    
    return success, dict_refSeq_AALength, dict_ENSEMBL_AALength, dict_CCDS_AALength



def place_transcript_and_AA_length_in_dictionary(transcript, AA_length, dictionary, type):
    success = 0
    try:
        if transcript != "\N":
            base_of_transcript = transcript.split(".")[0]
            if base_of_transcript in dictionary:
                if (type != "CCDS"):
                    sys.stderr.write("The transcript "+transcript+" is in the "+type+" dictionary more than once. Think about if this this allowed!!\n\n")
            else:
                dictionary[base_of_transcript] = [transcript, AA_length]
        success = 1
    except Exception, e:
        sys.stderr.write("An error occurred while in place_transcript_and_AA_length_in_dictionary() when the transcript = "+transcript+" and AA_lengh = "+AA_length+" and dictionary_type = "+type+"\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")         

    return success, dictionary


# Compare the uniProt_Ids transcript AA length to the respective CRAVAT transcripts AA_length based upon the convertion dictionary of 
# uniProt_Id to other database ID.
# Want to find other database transcripts that have the same length as the uniProt transcript
def get_CRAVAT_transcript_with_same_AA_length_to_uniProt_convertion_transcripts(dict_uniprotID_AA_length, dict_uniprot_to_Other_database_Ids, dict_refSeq_AALength, dict_ENSEMBL_AALength, dict_CCDS_AALength):
    success = 0
    dict_uniProtId_bestConvertionId = {}
    num_no_transfer = 0
    num_no_transfer_in_loop = 0
    num_uniprot = 0
    num_desired_no_cravat = 0
    exception_from_raise = 0
    
    dict_of_dicts_type_transcripts_length = {
                                             "RefSeq_NT" : dict_refSeq_AALength, 
                                             "Ensembl_TRS" : dict_ENSEMBL_AALength, 
                                             "CCDS": dict_CCDS_AALength
                                             }

    try:
#         Both dictionaries dict_uniprotID_AA_length and dict_uniprot_to_Other_database_Ids have the exact same keys so you can loop through either one
        for uniProtId in dict_uniprot_to_Other_database_Ids:
            num_uniprot += 1
            uniProt_transcript_length = dict_uniprotID_AA_length[uniProtId]
            dict_for_that_uniProt_id = dict_uniprot_to_Other_database_Ids[uniProtId]
            
            
            best_representative_transcript = None
            
#             MESSED UP SOMEHOW I THINK MAYBE!!!!!!
#           You might be missing some of the CRAVAT convertion abilities!!!
#             Loop replacing the more layed out below. Im pretty sure it's the same
            for db_type in array_convention_types_desired_in_order:
                if best_representative_transcript == None:
                    if db_type in dict_for_that_uniProt_id:
                        db_type_transcripts_for_uniProtId = dict_for_that_uniProt_id[db_type].split(",")
                        db_type_transcript_lengths = dict_of_dicts_type_transcripts_length[db_type]
                        no_fail_while_looking, best_representative_transcript = find_other_database_ID_with_same_length_as_uniProt_ID(uniProtId, db_type_transcripts_for_uniProtId, db_type_transcript_lengths, uniProt_transcript_length, db_type)
                        
                        if best_representative_transcript != None:
                            found_something = 1
                        if no_fail_while_looking == 0:
                            exception_from_raise = 1
                            raise Exception

            if best_representative_transcript == None:
#                 for db_type1 in array_convention_types_desired_in_order:
#                     if db_type1 in dict_uniprot_to_Other_database_Ids[uniProtId]:
#                         db_type1_transcripts_for_uniProtId = dict_uniprot_to_Other_database_Ids[uniProtId][db_type1].split(",")
#                         sys.stdout.write("%%%")
#                         sys.stdout.write(str(db_type1) + ":" + str(db_type1_transcripts_for_uniProtId)) 
#                 sys.stdout.write("\n")                 
                num_no_transfer_in_loop += 1
            else:
                dict_uniProtId_bestConvertionId[uniProtId] = best_representative_transcript
            
            
#             More layed out. This code was here before the loop above was made
# #             Go through RefSeq
#             if "RefSeq_NT" in dict_uniprot_to_Other_database_Ids[uniProtId]:
#                 refSeqIds_for_uniProtId = dict_uniprot_to_Other_database_Ids[uniProtId]["RefSeq_NT"].split(",")
#                 no_fail_while_looking, best_representative_transcript = find_other_database_ID_with_same_length_as_uniProt_ID(uniProtId, refSeqIds_for_uniProtId, dict_refSeq_AALength, uniProt_transcript_length)
# #                 print "uniprotId = "+uniProtId+"       best_rep_trans = " + str(best_representative_transcript)
#             if best_representative_transcript == None:
# #             Go through ENSEMBL
#                 if "Ensembl_TRS" in dict_uniprot_to_Other_database_Ids[uniProtId]:
#                     ensemblIds_for_uniProtId = dict_uniprot_to_Other_database_Ids[uniProtId]["Ensembl_TRS"].split(",")
#                     no_fail_while_looking, best_representative_transcript = find_other_database_ID_with_same_length_as_uniProt_ID(uniProtId, ensemblIds_for_uniProtId, dict_ENSEMBL_AALength, uniProt_transcript_length)
# #                     print "\tuniprotId = "+uniProtId+"       best_rep_trans = " + str(best_representative_transcript)
#             if best_representative_transcript == None:
# #             Go through CCDS
#                 if "CCDS" in dict_uniprot_to_Other_database_Ids[uniProtId]:
#                     ccdsIds_for_uniProtId = dict_uniprot_to_Other_database_Ids[uniProtId]["CCDS"].split(",")
#                     no_fail_while_looking, best_representative_transcript = find_other_database_ID_with_same_length_as_uniProt_ID(uniProtId, ccdsIds_for_uniProtId, dict_CCDS_AALength, uniProt_transcript_length)
# #                     print "\t\tuniprotId = "+uniProtId+"       best_rep_trans = " + str(best_representative_transcript)
#             if best_representative_transcript == None:
# #                 print str(dict_uniprot_to_Other_database_Ids[uniProtId])
#                 num_no_transfer += 1

        
        success = 1
    except Exception, e:
        if exception_from_raise == 1:
            pass
        else:
            sys.stderr.write("An error occurred while in get_CRAVAT_transcript_with_same_AA_length_to_uniProt_convertion_transcripts()\n")
            sys.stderr.write(str(repr(e)))
            sys.stderr.write("\n")
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("\n")  
    print "num_uniprot = " + str(num_uniprot)
    print "num_no_transfer_in_loop = " + str(num_no_transfer_in_loop)
   
    return success, dict_uniProtId_bestConvertionId

# This function goes through the other respective transcripts for a uniProtId and determines if they have the same length
# as the uniprot transcript length. This is done to choose the best other database transcript corresponding to the uniProt transcript
def find_other_database_ID_with_same_length_as_uniProt_ID(uniProtId, transcripts, dict_transcripts_length, uniprot_transcript_length, db_type):
    success = 0
    transcripts_with_same_length = []
    best_transcript = None
    try:

        for transcript in transcripts:
            base_transcript_id = transcript.split(".")[0]
            if base_transcript_id in dict_transcripts_length:
                cravat_full_transcript_id = dict_transcripts_length[base_transcript_id][0]
                other_transcript_length = dict_transcripts_length[base_transcript_id][1]
                if int(other_transcript_length) == int(uniprot_transcript_length):
                    #Save that transcript as the best transcript!!!
                    #Only do this for the first one that is run in to
                    #That is why you have the if best_transcript == None: thing
                    if best_transcript == None:
                        best_transcript = cravat_full_transcript_id   
                    
                    #This is to double check that there aren't two different representative transcripts that have the same length as the uniprot transcript.
#                     If there are that can cause trouble
                    transcripts_with_same_length.append(transcript)   
#             else:
#                 sys.stderr.write(transcript)
#                 pass

#     Right now you are only picking the first but you might want to see these both later!
        if len(transcripts_with_same_length) > 1:
#             YOU NEED TO PRINT THIS!!!!!!!!!!!!!!!            
#             sys.stderr.write("For the uniProId = "+str(uniProtId)+" with the length = "+str(uniprot_transcript_length)+".\n\t There is more than one respective transcript from another database that has the same length. The other transcripts are = "+str(transcripts_with_same_length) + "\n\n")
#             raise Exception
            pass
        

        success = 1
    except Exception, e:
        sys.stderr.write("An error occurred while in find_other_database_ID_with_same_length_as_uniProt_ID()\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")            
    
    return success, best_transcript



# Builds a table from the original UCSC table but adds columns for the AA_length of the uniProt transcript and the convertion IDs
# from the uniProt_Id to other database Ids
def write_full_uniprot_table(ucsc_table, uniprot_full_table, dict_uniProtId_bestConvertionId, dict_uniprotID_AA_length):
    success = 0
    try:
        rf = open(ucsc_table, 'rU')
        wf = open(uniprot_full_table, 'w')
        tok_titles = []
        seen_title_line = 0
        for line in rf:
            line = strip_new_line_characters(line)
            toks = split_by_tab(line)
            if seen_title_line == 0:
                tok_titles = toks
                wf.write(str(line))
                
                wf.write("\t" + "UniProt_AA_length")
                wf.write("\t" + "CRAVAT_Match")
                
                wf.write("\n")
                seen_title_line += 1
                continue
            
            toks_object = {}
            tok_num = 0
            for tok in toks:
                toks_object[tok_titles[tok_num]] = tok
                tok_num += 1
            # write the beginning of the line. The stuff that was already there
            wf.write(line)
            
            # retrieve the new info using the uniprotID
            uniprotId_for_line = toks_object["uniProtId"]
            
            # Write the uniprot length
            if uniprotId_for_line in dict_uniprotID_AA_length:
                wf.write("\t")
                wf.write(str(dict_uniprotID_AA_length[uniprotId_for_line])) 
            else:
                wf.write("\t")
                wf.write("No_Conversion_Detected") 
                
            if uniprotId_for_line in dict_uniProtId_bestConvertionId:
                
                wf.write("\t")
                wf.write(dict_uniProtId_bestConvertionId[uniprotId_for_line])
            else:
                wf.write("\t")
                wf.write("No_CRAVAT_Match")                        
            wf.write("\n")             
         
        rf.close()
        wf.close()
        success = 1
    except Exception, e:
        sys.stderr.write("An error occurred while in write_full_uniprot_table()\n")
        sys.stderr.write(str(repr(e)))
        sys.stderr.write("\n")
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("\n")         
        try:
            rf.close()
            wf.close()
        except Exception, f:
            sys.stderr.write("The read and write files could not be closed in write_full_uniprot_table()\n")
            sys.stderr.write(repr(f))
            sys.stderr.write("\n\n")
        
    return success




def parse_UCSC_table_with_conversion(table_with_conversions, database_table_input_file, cravat_transcripts, uid_hugo_cravat):
    import re
    AA_start_and_stop_regEx = re.compile('acids\s(\d+)-(\d+)\son')
    AA_single_position_regEx = re.compile('acid\s(\d+)\son')
    num_hugo_not_there = 0
    num_skipped_because_no_aaLength = 0
    num_skipped_because_no_conversion = 0
    try:
        rf = open(table_with_conversions, 'rU')
        wf = open(database_table_input_file, 'w')
        
#         wf.write("chrom\tfeature_key\tdescription\tstart\tstop\tuniprot_id\taa_len\tcravat_transcription\thugo\n")
        
        success_make_CRAVAT_transcript_dicts, dict_CRAVAT_refSeq_AALength, dict_CRAVAT_ENSEMBL_AALength, dict_CRAVAT_CCDS_AALength = make_dicts_CRAVAT_transcripts_AA_length(cravat_transcripts, 2)
        if success_make_CRAVAT_transcript_dicts == 0:
            raise Exeption
        
#         Make dictionary of UID to hugo symbol
        dict_uid_to_hugo = {}
        rf_2 = open(uid_hugo_cravat, 'rU')
        for line in rf_2:
            line = strip_new_line_characters(line)
            toks = split_by_tab(line)   
            dict_uid_to_hugo[toks[0]] = toks[1]       
        
                        
        title_line_hit = 0
        tok_titles = []
        for line in rf:
            line = strip_new_line_characters(line)
            toks = split_by_tab(line)
            if title_line_hit == 0:
                tok_titles = toks
                tok_titles[0] = tok_titles[0][1:]     #This removes the # symbol from the front
                title_line_hit = 1
                continue
            toks_object = {}
            tok_num = 0
            for tok in toks:
                toks_object[tok_titles[tok_num]] = tok
                tok_num += 1         
            
#             Get the chrom/chromosome
            chrom = toks_object["chrom"]
#             You have the following if statement so you only get real chromosomes
            if len(chrom) > 5:
                continue


#             Get the feature_key/annotationType
            feature_key = toks_object["annotationType"]
            
#             Get the description/comments
            description = toks_object["comments"]
            
#             Get the start and stop from position
            domain_location = toks_object["position"]
            start_pos = None
            end_pos = None
            AA_pos_match = AA_start_and_stop_regEx.search(domain_location)
            if AA_pos_match == None:
                AA_pos_match = AA_single_position_regEx.search(domain_location)
                if AA_pos_match == None:
                    print domain_location
                else:
                    start_pos = AA_pos_match.group(1)
                    end_pos = AA_pos_match.group(1)                
            else:
                start_pos = AA_pos_match.group(1)
                end_pos = AA_pos_match.group(2)
                
            
#             Get the uniprot_id/uniProtId
            uniprot_id = toks_object["uniProtId"]

#             Get the aa_len/UniProt_AA
            aa_len = toks_object["UniProt_AA_length"]
            if aa_len == "No_Conversion_Detected":
                num_skipped_because_no_aaLength += 1
                continue
#             Get cravat_transcript/CRAVAT_Match
            cravat_transcript = toks_object["CRAVAT_Match"]
            if cravat_transcript == "No_CRAVAT_Match":
                num_skipped_because_no_conversion += 1
                continue
            
#             get hugo
            base_trans = cravat_transcript.split(".")[0]
            uid = None
            if base_trans[0:4] == "CCDS":
                uid = dict_CRAVAT_CCDS_AALength[base_trans][1]
            elif base_trans[0:4] == "ENST":
                uid = dict_CRAVAT_ENSEMBL_AALength[base_trans][1]
            else:
                uid = dict_CRAVAT_refSeq_AALength[base_trans][1]
            
            hugo = None
            if uid in dict_uid_to_hugo:
                hugo = dict_uid_to_hugo[uid]
            else:
                num_hugo_not_there += 1
                hugo = ""
            
            wf.write(str(chrom) + "\t"  + str(feature_key) + "\t"  + str(description) + "\t" + str(start_pos) +"\t" + str(end_pos) +"\t" + str(uniprot_id) +"\t" + str(aa_len) +"\t" + str(cravat_transcript) + "\t" +str(hugo)+"\n")
        
        print "num_skipped_because_no_aaLength = " + str(num_skipped_because_no_aaLength)
        print "num_skipped_because_no_conversion = "  + str(num_skipped_because_no_conversion)
        print "num_hugo_not_there = " + str(num_hugo_not_there)
        rf.close()
        wf.close()
    except Exception, e:
        sys.stderr.write(str(repr(e)))
        sys.stderr.write(str(traceback.format_exc()))
        try:
            rf.close()
            wf.close()
        except NameError, p:
            sys.stderr.write(str(repr(p)))
           
#     load_mySQL_table(database_table_input_file) 
    
    return


def load_mySQL_table(input_file):
    import MySQLdb
    
    try:
        db = MySQLdb.connect(host=mysql_host,\
                             user=mysql_user,\
                             passwd=mysql_password,\
                             db=db_name)
        
        cursor = db.cursor() 

        
        stmt = 'drop table if exists '+table_name
        cursor.execute(stmt)
        db.commit()
        
        stmt = 'create table '+table_name+' ('+table_scheme+') engine=innodb'
        cursor.execute(stmt)
        db.commit()
        
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
            sys.stderr.write(str(repr(input_file)))
    return




# Strip the new line characters \n nad \r from the right hand side of an object and then return this stripped version
def strip_new_line_characters(object):
    stripped_object = object.rstrip("\r\n")
    return stripped_object

# Split an object up by tabs to make array and then return the array
def split_by_tab(object):
    split_object = object.split("\t")
    return split_object

# Remove white space characters from the beginning and end of an object
def remove_leading_trailer_white_space(object):
    object = object.lstrip()
    object = object.rstrip()
    return object
    
if __name__ == "__main__":

    input = sys.argv[1]

    dir = "/Users/derekgygax/Desktop/CRAVAT_Info_Test/InteractiveResultsPage/uniprot_transcript_retrieval/reDone/"

#     download these manually
    ucsc_table = dir + "UCSC_uniprot.bed"       #from UCSC
    uniprot_fasta = dir + "uniprot_sprot.fasta"     #from uniprot
    uniprot_to_other_databases_convertion_file = dir + "HUMAN_9606_idmapping.dat"       #from uniprot
    cravat_transcripts = dir + "CRAVAT_transcripts.txt"     #from Rick
    uid_hugo_cravat = dir + "uid_hugo.txt"                  #from Rick
    
#     these are created and used by this program
    distinct_uniprot_ids = dir + "distinct_human_uniProtIds.tsv"
    uniprot_full_table = dir + "Uniprot_and_Other_Database_Converstion.bed"
    database_table_input = dir + "database_table_input.tsv"
    
    
    if int(input) == 1:
    #         This builds a dictionary of all the unique UniProt id's from the table built by UCSC
        dict_unique_uniProtId = retrieve_uniProtIds(ucsc_table, distinct_uniprot_ids)
      
    #         This takes the table built by UCSC and adds columns for the corresponding refSeq Id and for the length of the amino acid
        construct_full_uniprot_table(ucsc_table, uniprot_to_other_databases_convertion_file, uniprot_fasta, uniprot_full_table, dict_unique_uniProtId, cravat_transcripts)
    elif int(input) == 2:
        parse_UCSC_table_with_conversion(uniprot_full_table, database_table_input, cravat_transcripts, uid_hugo_cravat)
    
    