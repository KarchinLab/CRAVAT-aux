# 
# 
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# # THIS PROGRAM HAS BECOME OBSOLETE!!!
# 
# import os
# import sys
# import datetime
# import time
# 
# 
# # This program converts gene cards that are in the old format, with two lines, into the new format with only one line
# # The time stamp for the genecard, the last time it was updated from genecards.org, will still remain the same
# 
# # Old gene card format:
# # entrez_annotation + \n
# # uniprot_annotation + \n
# 
# # New gene card foramt:
# # GeneCards Summary for (gene name) Gene] something | [Entrez Gene summary for (gene name) Gene] something | [UniProtKB/Swiss-Prot] something
# 
# 
# #These variables are constant and used throughout many of the functions. DO NOT!!! ever change this value or have a different variable with this same name!!!!!!!
# dir_genecards = '/home/dgygax/convert_genecards/geneCardAnnotCache/'
# entrez_beginning = '[Entrez Gene summary for'
# uniprot_beginning = '[UniProtKB/Swiss-Prot]'
# 
# def main():
#     
#     genecard_files = os.listdir(dir_genecards)
# #     print genecard_files
#     
# 
#     for genecard in genecard_files:
#   
# #     Imporant for mac. Can remove on linux
#         if genecard == ".DS_Store":
#             continue
#         
#         print 'Working on genecard ' + genecard
#         
# #         Retrieve the log time for the gene card (The time the card was last updated)
#         problem_save_seen, access_time, logged_time_originally = save_file_original_time_logged(genecard)
#         
#         if problem_save_seen == 1:
#             continue
#         
# #         Read the genecard contents and save them as one string
#         problem_read_seen, genecard_content_as_one_line = read_gene_card(genecard)
#         
#         if problem_read_seen == 1:
#             continue
# #         Write a new copy of the genecard as one string
#         new_genecard = genecard +'.new'
# 
#         problem_write_seen = rewrite_genecard(new_genecard, genecard_content_as_one_line)
#         
#         if problem_write_seen == 1:
#             continue
#         
# #         Convert the time log for the new_genecard to what the time log was for the genecard that had 2 lines
#         problem_change_time_seen = change_file_log_time(new_genecard, access_time, logged_time_originally)
#         
#         if problem_change_time_seen == 1:
#             continue
#     
#     return
# 
# # Save the time logged for the genecard
# # This is the time the genecard was last updated
# def save_file_original_time_logged(genecard_looking_at):
#     problem_occurred = 0  #This means no problem has occurred
#     try:
#         
#         from stat import ST_ATIME
#         from stat import ST_MTIME
#             
#         original_log_time = None
#             
#         path_to_genecard = dir_genecards + genecard_looking_at
#             
#         st = os.stat(path_to_genecard)
#             
#         accessed_time = st[ST_ATIME] #access time
#             
#             
#         #     This is the time displayed as the time stamp for the file
#         #     The one the mac shows and the one seen by ls -l
#         original_log_time = st[ST_MTIME] #modification time
#             
#         return problem_occurred, accessed_time, original_log_time
#     except Exception, bad:
#         problem_occurred = 1
#         sys.stderr.write('Problem occurred while retrieve the log time for the genecard: ' +genecard_looking_at +'. The problem was :\n\t'+bad+'\n\tOther genecards will be examined but a new remake genecard will not be made for this genecard.\n')
#         # Accessed time and orginal log time will be zero in this case becase an error was reached
#         return problem_occurred, 0, 0 
# 
# # Read the gene card and save the contents as one line
# # The first line is for Entrez and second line is for Uniprot
# def read_gene_card(gene_card):
#     problem_here = 0
#     try:
#     
#         card_as_one_line = ''  
#         
#         gene_name = gene_card.split('.')[0]
#         
#         card = open(dir_genecards+gene_card, 'r')
#             
#         line_on = 0
#     #     Keep track of whether the previous line is blank. 0 means NO not blank and 1 mean YES blank
#         prev_line_blank = 0         
#             
#         for line in card:
#             line_on += 1
#             
#             line = line.rstrip()
#             line = line.lstrip()
#             
#             
#     #         The variable 'entrez_beginning' is a constant global variable
#             if line != '': 
#                 if line_on == 1:
#                     card_as_one_line += entrez_beginning + ' ' +gene_name + ' Gene] '       # I don't know if I really should include this white space or not. I don't think it is neccesary really
#                                                                                             # but if would be worse to have now white space than have one too many. Considering they may be a circumstance where it is required
#                     card_as_one_line += line
#                 else:
#                     if prev_line_blank == 0:
#                         card_as_one_line += ' | '
#                 
#     #         The variable 'uniprot_beginning' is a constant global variable
#                 if line_on == 2:
#                     card_as_one_line += uniprot_beginning + ' '
#                     card_as_one_line += line
#                     
#                 prev_line_blank = 0     #This keeps track of if the line you are looking at is blank. This will be used in the next iteration of the loop
#             else:
#                 prev_line_blank = 1     #This keeps track of if the line you are looking at is blank. This will be used in the next iteration of the loop
#     
#             
#         card.close()
#         
#         return problem_here, card_as_one_line
#     except Exception, problem_reading:
#         problem_here = 1
#         sys.stderr.write('A problem has occurred when reading the genecard for ' + gene_card + '. The problem was:\n\t' +problem_reading+ '\n\tOther genecards will be looked at but no genecard remake will be made for this genecard.\n')
#         try:
#             card.close()
#         except Exception, e:        #You will only get an except here if the file 'card' hasn't been opened yet. If it hasn't then just move on
#             b = 4  #Just a place holder
#         
#         return problem_here, 0      #If a problem has occurred then the variable 'card_as_one_line' will be returned as 0
# 
# # Re-write the genecard using the single string holding the contents of the genecard
# def rewrite_genecard(new_gene_card, genecard_oneline):
#     problem_here = 0
#     try:
#         #Make a new genecard that has the extension '.new at the end
#         #The old one will stay there then and a new one will be made additionally
#     
#         new_card = open(dir_genecards + new_gene_card, 'w')      
#         new_card.write(genecard_oneline)
#         new_card.close()
#         return problem_here
#     except Exception, aa:
#         sys.stderr.write('A problem has occurred while attempting to make the new genecard remake ' + new_gene_card + '. The problem was:\n\t' +aa+ '\n\tThe new genecard will not be created or not be filled correctly. Note this mistake. Conversion of other genecards will be continued.\n')
#         problem_here = 1
#         try:
#             new_card.close()
#         except Exception, bb:
#             c = 7 #Place holder
#         return problem_here
#             
# 
# # Convert the time log of the file back from the update that just took place to 
# # the log time of the previous update. This is done so very old genecards will still
# # be registered as old
# def change_file_log_time(gene_card, accessed_time, log_time):
#     problem_say_here = 0
#     try:
#         
#         path_to_genecard = dir_genecards + gene_card
#         
#         os.utime(path_to_genecard,(accessed_time, log_time))
#         
#         return problem_say_here
#     except Exception, no_change_time:
#         problem_say_here = 1
#         sys.stderr.write('A problem has occurred while attempting change the log time for the new genecard ' + gene_card + '. The problem was:\n\t' +no_change_time+ '\n\t The log time of this new genecard is incorrect. Conversion of other genecards will be continued, however note this mistake.\n')
#         return problem_say_here
# 
# if __name__ == "__main__":
#     main()