#!/usr/bin/python

import datetime
import sys,os
import time
import urllib
import HTMLParser
import smtplib

class Updater ():

    pubmed_dir = '/export/karchin-web02/CRAVAT_resource/pubmedSearchResultCache'

    expiration = 100 # 100 days

    max_no_of_files_to_update = 100

    logfile = '/home/rkim/cron_jobs/update_pubmed_cache_log.txt'

    def __init__(self):
        pass

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

            log_str = ''

            #
            # PubMed update
            #

            log_str += 'PubMed update started\n'

            # Gathers Gene symbols from the PubMed cache files.
            cache_files = os.listdir(self.pubmed_dir)

            # Pick the 200 oldest ones: it will take 99 days to update all the cache files (assuming that there are 19874 Gene IDs).
            cache_files_to_update = self.get_oldest_files(self.pubmed_dir, cache_files)

            # Cycle through the 200 oldest cache files and update them.
            for cache_file in cache_files_to_update:
                hugo = cache_file.split('.')[0]
                log_str += cache_file + '\t' + str(datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(self.pubmed_dir, cache_file)))) + '\n'
                self.update_pubmed_cache(hugo)
                time.sleep(300) # Sleeps for 5 minutes.

            log_str += 'PubMed updated\n'

            for cache_file in cache_files_to_update:
                log_str += cache_file + '\n' + str(datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(self.pubmed_dir, cache_file)))) + '\n'

            log_str += 'Update completed\n'

            wf = open(self.logfile, 'w')
            wf.write(log_str)
            wf.close()

        except Exception, e:
            self.send_exception_email(log_str)

    def send_exception_email (self, log_str):
        import traceback
        tb = traceback.format_exc()

        from email.mime.text import MIMEText
        subject = 'Exception: PubMed cache update'
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

    def update_pubmed_cache (self, gene_symbol):
        try:
            gene_symbol_and_search_term = gene_symbol+'[MH]'
            n = self.get_pubmed_search_result_number(gene_symbol_and_search_term)
            if n == -1:
                return
            elif n == 0:
                gene_symbol_and_search_term = gene_symbol+'[TIAB]'
                n = self.get_pubmed_search_result_number(gene_symbol_and_search_term)
                if n == -1:
                    return
                elif n == 0:
                    gene_symbol_and_search_term = ''

            if n == -1:
                return
            elif n == 0:
                search_term = ''
            else:
                search_term = gene_symbol_and_search_term+'+AND+cancer[MH]'
                n = self.get_pubmed_search_result_number(search_term)
                if n == -1:
                    return
                elif n == 0:
                    search_term = gene_symbol_and_search_term + '+AND+cancer[TIAB]'
                    n = self.get_pubmed_search_result_number(search_term)
                    if n == -1:
                        return
                    elif n == 0:
                        search_term = ''

            if n != -1:
                write_flag = True
                pubmed_search_filename = os.path.join(self.pubmed_dir, gene_symbol+'.pubmedSearch')
                if os.path.exists(pubmed_search_filename):
                    f = open(pubmed_search_filename)
                    f.readline()
                    line = f.readline()
                    if line.isdigit() == True and int(line) > 0 and n <= 0:
                        write_flag = False
                
                if write_flag == True:
                    wf = open(pubmed_search_filename, 'w')
                    wf.write(search_term+'\n')
                    wf.write(str(n))
                    wf.close()
        except Exception, e:
            raise

    def get_pubmed_search_result_number(self, search_term):
        try:
            from Bio import Entrez
            Entrez.email = 'CRAVAT_DoNotReply@karchinlab.org'
            h = Entrez.esearch(db='pubmed',term=search_term)
            result = Entrez.read(h)
            count = int(result['Count'])
            return count
        except Exception, e:
            raise

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
