import shutil
import sys
import time
import urllib
import HTMLParser
import os
import logging

class Updater:
    genecard_annotation_cache_dir = '/export/karchin-web02/CRAVAT_resource/geneCardAnnotCache'
    
    def __init__ (self):
        logging.basicConfig(format='%(levelname)s:%(name)-30s:%(lineno)4d:%(asctime)s:%(message)s')
        self.logger = logging.getLogger('genecardsupdater')
        self.logger.setLevel(logging.DEBUG)
        
    def update_genecards_annotation_cache (self, hugo):
        try:
            if os.path.exists(self.genecard_annotation_cache_dir) == False:
                os.makedirs(self.genecard_annotation_cache_dir)
            
            genecard_annotation_filename = os.path.join(self.genecard_annotation_cache_dir, hugo+'.geneCardAnnot')
            
            if True: #os.path.exists(genecard_annotation_filename) == False:
                url = 'http://bioinformatics.mdanderson.org/genecards/cgi-bin/carddisp.pl?gene=' + hugo
                f=urllib.urlopen(url)
                htmlLines = f.read()
                f.close()
                
                try:
                    summaries_start_pos = htmlLines.index('<h2 class="navbar">Summaries')
                    summaries_end_pos = htmlLines.index('<h2 class="navbar">Genomic Views')
                    htmlLines = htmlLines[summaries_start_pos:summaries_end_pos]
                except ValueError:
                    wf = open(genecard_annotation_filename,'w')
                    wf.write('\n')
                    wf.write('\n')
                    wf.close()
                    return False
                
                try:
                    entrez_start_pos = htmlLines.index('Entrez Gene summary for')
                except ValueError:
                    entrez_start_pos = -1
                if entrez_start_pos > 0:
                    htmlLines = htmlLines[entrez_start_pos:]
                    entrez_start_pos = htmlLines.index('<dd>')+4
                    try:
                        htmlLines = htmlLines[entrez_start_pos:]
                        entrez_end_pos = htmlLines.index('<br >')
                        entrezLines = htmlLines[:entrez_end_pos].rstrip(' ')
                        entrez_annotation = self.remove_html_tags(entrezLines).rstrip(' ')
                    except Exception, e:
                        entrez_annotation = ''
                else:
                    entrez_annotation = ''
                try:
                    uniprot_start_pos = htmlLines.index('UniProtKB/Swiss-Prot')
                except ValueError:
                    uniprot_start_pos = -1
                if uniprot_start_pos > 0:
                    htmlLines = htmlLines[uniprot_start_pos:]
                    try:
                        uniprot_start_pos = htmlLines.index('<dd>')+4
                    except Exception, e:
                        uniprot_start_pos = -1
                    if uniprot_start_pos >= 0:
                        uniprot_end_pos = htmlLines.index('<br >')
                        uniprotLines = htmlLines[uniprot_start_pos:uniprot_end_pos].rstrip(' ')
                        uniprot_annotation = self.remove_html_tags(uniprotLines).rstrip(' ')
                    else:
                        uniprot_annotation = ''
                else:
                    uniprot_annotation = ''
                wf = open(genecard_annotation_filename,'w')
                wf.write(entrez_annotation+'\n')
                wf.write(uniprot_annotation+'\n')
                wf.close()
                return True
        except Exception, e:
            self.logger.exception('Exception occurred at geneannotator.py:get_genecards_annotation')
            raise Exception

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
        
    def run (self):
        f = open('genesymbols.txt') # Pulled from GeneSymbols table.
        hugos = [hugo.strip() for hugo in f.readlines()]
        f.close()
        
        count = 0
        for hugo in hugos:
            count += 1
            print count, hugo,
            ret = self.update_genecards_annotation_cache(hugo)
            if ret == True:
                print ' ...Done'
            else:
                print ' ...Exception'
            time.sleep(20)
        
    def get_missing_cache (self):
        f = open('genesymbols.txt') # Pulled from GeneSymbols table.
        hugos = [hugo.strip() for hugo in f.readlines()]
        f.close()
        
        missing_hugos = []
        for hugo in hugos:
            genecard_annotation_filename = os.path.join(self.genecard_annotation_cache_dir, hugo+'.geneCardAnnot')
            if os.path.exists(genecard_annotation_filename) == False:
                missing_hugos.append(hugo)
        print len(missing_hugos), 'HUGOs missing'
        
        count = 0
        for hugo in missing_hugos:
            count += 1
            print count, hugo,
            ret = self.update_genecards_annotation_cache(hugo)
            if ret == True:
                print ' ...Done'
            else:
                print ' ...Exception'
            time.sleep(1)

    def update_zero_size_cache (self):
        geneid_to_hugo = {}
        f = open('hugo_geneid.txt')
        for line in f:
            [hugo, geneid] = line.strip().split('\t')
            geneid_to_hugo[geneid] = hugo
        f.close()
        
        count = 1
        for hugo in geneid_to_hugo.values():
            print count, hugo
            cache_filename = os.path.join(self.genecard_annotation_cache_dir, hugo+'.geneCardAnnot')
            f = open(cache_filename)
            entrez_annot = f.readline().strip()
            uniprot_annot = f.readline().strip()
            f.close()
            if entrez_annot == '' and uniprot_annot == '':
                self.update_genecards_annotation_cache(hugo)
            count += 1
        
    def rename_geneid_to_hugo_cache (self):
        geneid_to_hugo = {}
        f = open('hugo_geneid.txt')
        for line in f:
            [hugo, geneid] = line.strip().split('\t')
            geneid_to_hugo[geneid] = hugo
        f.close()
        
        for geneid in geneid_to_hugo.keys():
            hugo = geneid_to_hugo[geneid]
            geneid_cache_filename = os.path.join(self.genecard_annotation_cache_dir, geneid+'.geneCardAnnot')
            hugo_cache_filename = os.path.join(self.genecard_annotation_cache_dir, hugo+'.geneCardAnnot')
            
            if os.path.exists(geneid_cache_filename):
                if os.path.exists(hugo_cache_filename) == False:
                    print hugo, ':', 'Move', geneid_cache_filename, 'to', hugo_cache_filename
                    shutil.move(geneid_cache_filename, hugo_cache_filename)
                else:
                    f = open(hugo_cache_filename)
                    hugo_cache_entrez_annot = f.readline().strip()
                    hugo_cache_uniprot_annot = f.readline().strip()
                    f.close()
                    hugo_cache_annot = hugo_cache_entrez_annot + hugo_cache_uniprot_annot
                    
                    f = open(geneid_cache_filename)
                    geneid_cache_entrez_annot = f.readline().strip()
                    geneid_cache_uniprot_annot = f.readline().strip()
                    f.close()
                    geneid_cache_annot = geneid_cache_entrez_annot + geneid_cache_uniprot_annot
                    
                    if hugo_cache_annot == '' and geneid_cache_annot != '':
                        print hugo, ':', 'Overwrite', hugo_cache_filename, 'with', geneid_cache_filename
                        shutil.move(geneid_cache_filename, hugo_cache_filename)
                    elif hugo_cache_annot == '' and geneid_cache_annot == '':
                        print hugo, ':', 'Do nothing.', geneid_cache_filename, ' and ', hugo_cache_filename, 'are both no-content'
                        print '  Deleting', geneid_cache_filename
                        os.remove(geneid_cache_filename)
                    elif hugo_cache_annot != '' and geneid_cache_annot == '':
                        print hugo, ':', 'Do nothing.', geneid_cache_filename, 'is not better than', hugo_cache_filename
                        print '  geneid_cache: [', geneid_cache_annot, ']'
                        print '  hugo_cache [', hugo_cache_annot, ']'
                        print '  Deleting', geneid_cache_filename
                        os.remove(geneid_cache_filename)
                    elif hugo_cache_annot != '' and geneid_cache_annot != '':
                        print hugo, ':', 'Both content:', geneid_cache_filename, hugo_cache_filename
                        print '  geneid_cache: [', geneid_cache_annot, ']'
                        print '  hugo_cache [', hugo_cache_annot, ']'
                        if len(geneid_cache_annot) > len(hugo_cache_annot):
                            print '  Overwriting. geneid_cache has more content.'
                            shutil.move(geneid_cache_filename, hugo_cache_filename)
                        else:
                            print '  Deleting (less content)', geneid_cache_filename
                            os.remove(geneid_cache_filename)
            else:
                print hugo, ':', 'No cache:', geneid_cache_filename
            
updater = Updater()
# updater.run()
# updater.rename_geneid_to_hugo_cache()
# updater.get_missing_cache()
updater.update_zero_size_cache()