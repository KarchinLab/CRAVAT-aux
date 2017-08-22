#!/usr/bin/python

import datetime
import sys,os
import time
import urllib
import HTMLParser
import smtplib
#BeautifulSoup is now parsing with 'html5lib'
from bs4 import BeautifulSoup  #This is important for walking through a html document
import logging
from logging.handlers import RotatingFileHandler


# This still leaves extra white space before periods like this . 
# That is not a very big deal and can be fixed later

class Updater ():

    genecards_dir = '/export/karchin-web02/CRAVAT_resource/geneCardAnnotCache'


    

    expiration = 100 # 100 days


    max_no_of_files_to_update = 200

    logfile = '/home/rkim/cron_jobs/update_genecards_cache_log.txt'

    def __init__(self):
        pass

#This identifies files that have passed expiration.
#If the number of file exceeds the number allowed to be updated then
#I believe only the oldest up to the number allowed to be updated are kept in a dictionary and sent back to be updated
    def get_oldest_files (self, d, cache_files):

        try:
            expiration = 60 * 60 * 24 * self.expiration # 100 days

            now = time.time()

            cache_file_timestamp = {}
            for cache_file in cache_files:
                age = now - os.path.getmtime(os.path.join(d, cache_file))
                if age > expiration:
                    cache_file_timestamp[cache_file] = age

            if len(cache_file_timestamp) <= self.max_no_of_files_to_update:
                cache_files_to_update = cache_file_timestamp.keys()
            else:
                cache_files_to_update = sorted(cache_file_timestamp, key=cache_file_timestamp.get, reverse=True)[:self.max_no_of_files_to_update]

            return cache_files_to_update
        except Exception, e:
            raise
        

        

    def run (self):

        try:
            import datetime
            
            time_right_now = datetime.datetime.now()

            log_str = ''

            #
            # GeneCards update
            #

            
            log_str += 'GeneCards update started at '+str(time_right_now)+'\n'


            # Gathers Gene IDs (file names) from the GeneCards cache directory.
            cache_files = os.listdir(self.genecards_dir)

#             print cache_files
            
            
            
            # Pick the 200 oldest ones: it will take 95 days to update all the cache files (assuming that there are 19081 Gene IDs).
            cache_files_to_update = self.get_oldest_files(self.genecards_dir, cache_files)


#             cache_files_to_update = cache_files

            
            # Cycle through the 200 oldest cache files and update them.
            for cache_file in cache_files_to_update:
                
            
                
                gene_id = cache_file.split('.')[0]

                
                print 'Started working on ' + gene_id
                 
                log_str += cache_file + '\t' + str(datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(self.genecards_dir, cache_file)))) + '\n'

                self.update_genecards_cache(gene_id)
                time.sleep(300) ##########################################     This SHOULD BE 600!!!! For 10 min. OR 300 for 5 min

                print '\tFinished working on ' + gene_id

            log_str += 'GeneCards updated\n'

            

            

            for cache_file in cache_files_to_update:
                tmp_x = 5
                
                log_str += cache_file + '\t' + str(datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(self.genecards_dir, cache_file)))) + '\n'            

            log_str += 'Update completed\n'

            wf  = logging.getLogger("Rotating Log")
            wf.setLevel(logging.INFO)
             
            rotating_file_handler = RotatingFileHandler(self.logfile, maxBytes=133000, backupCount=3)       #This should make one log file hold a weeks worth of updates at 200 each day. There should be four logs, so one months worth of updates will be logged
 
            wf.addHandler(rotating_file_handler)
            wf.info('________________________________________________________________________\n' )
            wf.info(log_str)


        except Exception, e:
            print e
            
            self.send_exception_email(log_str)

    def send_exception_email (self, log_str):
        import traceback
        tb = traceback.format_exc()

        from email.mime.text import MIMEText
        subject = 'Exception: GeneCards cache update'
        body = log_str + '\n' + tb
        sender = 'CRAVAT_DoNotReply@karchinlab.org'
        recipient = 'rkim@insilico.us.com'
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient

        s = smtplib.SMTP('smtp.johnshopkins.edu')
        s.sendmail(sender, recipient, msg.as_string())
        s.quit()

        wf = open(self.logfile, 'w')
        wf.write(str(msg))
        wf.close()


# This uses the import 'BeautifulSoup' to go through the genecards.org web page for a gene and pick out the summary information
    def update_genecards_cache (self, gene_id):
        import re
        
        summaryRegEx = re.compile('Summaries')
        alpha_numberic_symbol_regex = re.compile('\W')
#         Under the summaries section in genecards one of the sections is for Tocris. You are using a regex to find this in the title so you can label the section Tocris
        to_cris_regex = re.compile('Tocris')
        wiki_entry_regex = re.compile('Wiki\sentry')
        
        try:
          
            # Reads GeneCards from the web.
            # Using genecards.org now
            url = 'http://www.genecards.org/cgi-bin/carddisp.pl?gene='+gene_id
            f=urllib.urlopen(url)

            
            #Convert the html into BeautifulSoup so you can navigate through it like in JQuery
            #BeautifulSoup is now parsing with 'html5lib'
            html_as_soup = BeautifulSoup(f)     #BeautifulSoup is now parsing with 'html5lib'
            
            f.close()
            time.sleep(1)
            
            h2_tag_for_summaries_section = None
            
            num_problems = 0
#             Search through all the h2 elements to find the one labeled with 'Summaries'
            for h2_elements in html_as_soup.find_all('h2'):
                num_problems += 1
                try:
                    
#                     The h2 we are looking for is in the needed row of a table and contains the word 'Summaries' so using regular expression I am looking for 'Summaries'
                    if re.search(summaryRegEx, h2_elements.text):
                        h2_tag_for_summaries_section = h2_elements
#                         print h2_tag_for_summaries_section
                    

                except Exception, eff:
                    print 'number ' +num_problems+ ' problem occurred while looking for the h2 tag signaling "Summaries":   \n\t' + str(eff)
                    


            #Variable 'full_summary_description will hold all the summary information for a gene used to fill the genecard
            full_summary_description = ''
            
            #Variable 'look_at_column' will hold a value deciding if the column should be examined for summary information. This is determined based on if a h2 with the test 'Summaries' was seen
            look_at_column = 0     #By default you are not looking at the row
            if h2_tag_for_summaries_section == None:
                look_at_column = 0     # If the h2 tag doesn't exist for the genecard/gene examining then looking for the column holding
            else:
                look_at_column = 1     # Else look at the row and full_summary_description will be changed
                      
#             Only if the h2 tag 'Summaries' existed do you look for the column
            if look_at_column == 1:
#             The column holding the data you want is one parent up and then two siblings across (next) from the h2 you found                    
                column_with_data =  h2_tag_for_summaries_section.parent.next_sibling.next_sibling       
                

#             Loop through the children in the desired column
#             The children with the name b or font (the UniProt ones have the font change) are the titles and the following dd's are the contents of the sections with that title until the next b is run into
                num_title_seen = 0
                line_content_in_one_content = 0
                skip_till_next_title = 0
                for child in column_with_data.children:
                    title = ''
                    content = ''
                    
#                     <b> signifies a title and then <font> signifies where a title will be located for UniProt
                    if child.name == 'b' or child.name == 'font':
                        
                        line_content_in_one_content = 0             #Every time you reach a new title restart the line_content_in_one_content count                        
# If in the html <b> or <font> is seen so child.name = b OR child.name = font in BeautifulSoup then the contents of that tag are the title of a section
                        title = str(child)                       
                         
                        title_is_for_wiki = re.search(wiki_entry_regex, title)
                        if title_is_for_wiki:
                            skip_till_next_title = 1
                            continue
                        else:
                            skip_till_next_title = 0
                            
                        
                        num_title_seen += 1
                        if num_title_seen > 1:
                            full_summary_description+= ' | '
# If in the html <b> or <font> is seen so child.name = b OR child.name = font in BeautifulSoup then the contents of that tag are the title of a section
                        title = str(child)
# If the title has the word 'Tocris' in one of the html tags then the word 'Tocris' will be added to the beginning of the title 
# This will occur AFTER the html tags have been removed
                        title_has_Tocris = re.search(to_cris_regex, title)
        
# Remove all the html tags so only the string contents are left
                        title = self.remove_html_tags(title)
# If 'Tocris' was seen in the html tags before they were removed add 'Tocris' to the beginning of the title
                        if title_has_Tocris:
                            title = 'TOCRIS ' + title
                        title = ' '.join(title.split())     #This takes all extra white space, \t, and \n, and removes them and makes it only 1 white space
                        
# Put square brackets around the title
                        title = '[' + title + '] '
# If :] exist then replace with ]
                        title = title.replace(':]', ']')
#If ' ]' then turn it into ']'                        
                        title = title.replace(' ]', ']')
# Put the title in the full_summary_description
                        full_summary_description += title
                        
                        
# If the title is for the 'Wiki entry' then the following lines will be skipped unless the contain a <b> or <font>
                    if skip_till_next_title == 1:
                        continue    
                        
                        
                    if child.name == 'dd':
                        line_content_in_one_content += 1
# If in the html <b> is seen so child.name = 'dd' in BeautifulSoup then the contents of that tag are the contents of the section   
                        content = str(child)
# Remove all the html tags so only the string contents are left            
                        content = self.remove_html_tags(content)
                        
# Clean up the output
                        content = ' '.join(content.split())         #This takes all extra white space, \t, and \n, and removes them and makes it only 1 white space
                        content = content.replace(' .', '.')
                        
# Put the content after the title
                    # If there is more than one line so <dd> in the content put a white space between the <dd> sections
                        if line_content_in_one_content > 1:     
                            full_summary_description += ' '
                            
    # Always put the content next in the 'full_summary_description'
                        full_summary_description += content
                        
                
#             print full_summary_description
            
            # Writes the GeneCards cache file.
            genecard_annotation_filename = os.path.join(self.genecards_dir, gene_id+'.geneCardAnnot')

            write_flag = True
             
#             This checks if there already is a gene card. If the gene card is already there and contains writing but the new one would be blank then
#             the new gene card in not created. So the old genecard will still stay there
            if os.path.exists(genecard_annotation_filename):
                ft = open(genecard_annotation_filename)
                existing_has_something = 0  #This means NO
                for line_in_exisitng in ft:
                    # First look for any alphanumberic symbol in the entire genecard. If there isn't then that will be noted for the next 'if'
                    has_alpha_numberic = re.search(alpha_numberic_symbol_regex, line_in_exisitng)
                    if has_alpha_numberic:
                        existing_has_something = 1
                
#                 This checks if the original gene card is filled in, and if the new one would be blank.
#                 If original genecard is filled in but the new one after updating is blank then no new genecard being made, and the old one still stands. 
#                 
                if (existing_has_something == 1 and full_summary_description == ''):  #If the existing card has something and the new one is blank then don't do anything
                    write_flag = False
                     
                ft.close()   #Closing the opened file
                     
            if write_flag == True:
                wf = open(genecard_annotation_filename,'w')
                wf.write(full_summary_description)
                wf.close()
        except Exception, e:
            raise
        


# This just skips any area in between '<' and '>' that
    def remove_html_tags (self, lines):
        try:
            annot_l = []
            pos = 0
            while pos < len(lines):
                c = lines[pos]
                if c == '<':
                    annot_l.append(' ')
                    pos += 1
                    while True:
                        c = lines[pos]
                        if c == '>':
                            break
                        pos += 1
                else:
                    annot_l.append(c)
                pos += 1
            return ''.join(annot_l)
        except Exception, e:
            raise

updater = Updater()
updater.run()
