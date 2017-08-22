import imaplib
import email
import os
import shutil
import sys
import time
import urllib
import zipfile
sys.path.append('/Users/derekgygax/Desktop/CRAVAT_MuPIT/CRAVAT/WebContent/WEB-INF/Wrappers/lib/python2.5/site-packages')
import xlrd
import xlwt

class VerifyTest:
    def __init__ (self, comparison_level, comp1, comp2):
        self.comparison_level = comparison_level
        self.comp1 = comp1
        self.comp2 = comp2
        (source_dir, dummy) = os.path.split(__file__)
        self.source_dir = source_dir
        self.result_base_dir = os.path.join(self.source_dir, 'result_downloads')
        if self.comparison_level == 'run':
            self.result_dir_1 = os.path.join(self.result_base_dir, self.comp1)
            self.result_dir_2 = os.path.join(self.result_base_dir, self.comp2)
        elif self.comparison_level == 'job':
            self.result_file_path_1 = self.comp1
            self.result_file_path_2 = self.comp2
        
    def close_spreadsheets (self):
        self.book1.release_resources()
        self.book2.release_resources()

    def compare_data_values (self, values_1, values_2, column_headers):
        uids_1 = values_1.keys()
        uids_2 = values_2.keys()
        
        uids_missing_in_1 = []
        uids_missing_in_2 = []
        common_uids = []
        
        for uid in uids_1:
            if values_2.has_key(uid) == False:
                uids_missing_in_2.append(uid)
            else:
                common_uids.append(uid)
        for uid in uids_2:
            if values_1.has_key(uid) == False:
                uids_missing_in_1.append(uid)
            else:
                if common_uids.count(uid) == 0:
                    common_uids.append(uid)
        
        if uids_missing_in_1 != [] or uids_missing_in_2 != []:
            print '#    Lonely UIDs', self.comp1, ':'
            for uid in uids_missing_in_2:
                print '#      ', uid
            print '#    Lonely UIDs', self.comp2, ':'
            for uid in uids_missing_in_1:
                print '#      ', uid
            #print '#    Common UIDs', common_uids
        
        # Detects different cell values.
        dif = {}
        dif_column_headers = []
        for uid in common_uids:
            row_1 = values_1[uid]
            row_2 = values_2[uid]
            for column_header in column_headers:
                if column_header.find('GeneCards') >= 0 or column_header.find('PubMed') >= 0:
                    continue
                if column_header.find('Occurrences in study') >= 0:
                    continue
                colno_1 = self.column_header_to_colno_1[column_header]
                colno_2 = self.column_header_to_colno_2[column_header]
                value_1 = str(row_1[colno_1]).strip()
                value_2 = str(row_2[colno_2]).strip()
                if column_header.find('ESP') >= 0 or column_header.find('1000 Genome') >= 0:
                    if (value_1 == '' and value_2 == '0.0') or (value_1 == '0.0' and value_2 == ''):
                        continue 
                if column_header.find('score of representative transcript') >= 0:
                    if value_1.find('(') >= 0:
                        value_1 = value_1.split('(')[0]
                    if value_2.find('(') >= 0:
                        value_2 = value_2.split('(')[0]
                if value_1 == '.':
                    value_1 = ''
                if value_2 == '.':
                    value_2 = ''
                if value_1 != value_2:
                    if dif.has_key(column_header) == False:
                        dif[column_header] = []
                        dif_column_headers.append(column_header)
                    dif[column_header].append([uid, value_1, value_2])
        
        # Prints out the different cell values.
        for column_header in dif_column_headers:
            print '#   ', column_header
            for v in dif[column_header]:
                [uid, value_1, value_2] = v
                print '       ', uid, '[' + value_1 + '], [' + value_2 + ']'
        
    def get_data_values (self, tab, column_headers, start_rowno, end_rowno):
        values = {}
        for rowno in xrange(start_rowno, end_rowno):
            row_values = tab.row_values(rowno)
            uid = ':'.join([str(v) for v in row_values[1:2]])
            values[uid] = row_values
        return values
    
    def compare_column_headers (self, column_headers_1, column_headers_2):
        common_column_headers = []
        no_match_column_headers_1 = []
        no_match_column_headers_2 = []
        for column_header in column_headers_1:
            if column_header in column_headers_2:
                common_column_headers.append(column_header)
            else:
                no_match_column_headers_1.append(column_header)
        for column_header in column_headers_2:
            if column_header in column_headers_1:
                pass
            else:
                no_match_column_headers_2.append(column_header)
        if len(no_match_column_headers_1) > 0:
            print '#    Lonely columns', self.comp1, ':'
            for column_header in no_match_column_headers_1:
                print '#      ', column_header
        if len(no_match_column_headers_2) > 0:
            print '#    Lonely columns', self.comp2, ':'
            for column_header in no_match_column_headers_2:
                print '#      ', column_header
        
        return common_column_headers

    def get_column_headers (self, tab):
        nrows = tab.nrows
        rowno = 0
        column_headers = None
        self.data_from_column_no = 1
        for rowno in xrange(nrows):
            row_values = tab.row_values(rowno)
            cell0 = row_values[0].lower()
            if cell0 in ['input line number', 'hugo symbol']:
                column_headers = row_values
                rowno += 1
                break
        column_header_to_colno = {}
        column_header_same_dic = {'Pathogenicity score of representative transcript (close to 1 means greater effect)':'Functional score of representative transcript (close to 1 means functional effect)', \
                                  'Functional score (close to 1 means functional effect)':'Pathogenicity score (close to 1 means greater effect)', \
                                  'Best functional score from representative transcripts':'Best pathogenicity score from representative transcripts', \
                                  'Driver score: FDR (Benjamini-Hochberg) (not available with less than 10 unique mutations)':'FDR (Benjamini-Hochberg) (not available with less than 10 unique mutations)', \
                                  'Pathogenicity score: FDR (Benjamini-Hochberg) (not available with less than 10 unique mutations)':'FDR (Benjamini-Hochberg) (not available with less than 10 unique mutations)', \
                                  'Driver score: Empirical p-value':'Empirical p-value', \
                                  'Pathogenicity score: Empirical p-value':'Empirical p-value', \
                                  #'Best driver score transcript':'Best driver score and transcript', \
                                  #'Best pathogenicity score transcript':'Best functional score and transcript', \
                                  'Driver score: All transcripts and scores':'All transcripts and driver scores', \
                                  'Pathogenicity score: All transcripts and scores':'All transcripts and functional scores', \
                                  'ExAC allele frequency (American Allele)':'ExAC AF (Latino)', \
                                  'ExAC allele frequency (East Asian Allele)':'ExAC AF (East Asian)', \
                                  'ExAC allele frequency (Finnish Allele)':'ExAC AF (Finnish)', \
                                  'ExAC allele frequency (Non-Finnish European Allele)':'ExAC AF (Non-Finnish European)', \
                                  'ExAC allele frequency (Other Allele)':'ExAC AF (Other)', \
                                  'ExAC allele frequency (South Asian Allele)':'ExAC AF (South Asian)', \
                                  'Transcript':'Reference transcript (priority to coding change)', \
                                  'Sequence Ontology':'Reference transcript sequence Ontology', \
                                  'Protein sequence change':'Reference transcript protein sequence change', \
                                  }
        for colno in xrange(len(column_headers)):
            column_header = column_headers[colno]
            if column_header_same_dic.has_key(column_header):
                column_header = column_header_same_dic[column_header]
                column_headers[colno] = column_header
            column_header_to_colno[column_header] = colno
            
        return (column_headers, column_header_to_colno, rowno, nrows)

    def compare_tab (self, tab1, tab2):
        # Gets column headers.
        (column_headers_1, self.column_header_to_colno_1, rowno_1, nrows_1) = self.get_column_headers(tab1)
        (column_headers_2, self.column_header_to_colno_2, rowno_2, nrows_2) = self.get_column_headers(tab2)
    
        # Gets common column headers.
        common_column_headers = self.compare_column_headers(column_headers_1, column_headers_2)
        
        # Reads data cell values.
        rowno_1 += 1
        values_1 = self.get_data_values(tab1, column_headers_1, rowno_1, nrows_1)
        rowno_2 += 1
        values_2 = self.get_data_values(tab2, column_headers_2, rowno_2, nrows_2)

        # Compares the values.
        self.compare_data_values(values_1, values_2, common_column_headers)
        
    def compare_common_tabs (self):
        for tab_name in self.common_tab_names:
            print '#  Tab:', tab_name
            tab1 = self.tabs_1[tab_name]
            tab2 = self.tabs_2[tab_name]
            self.compare_tab(tab1, tab2)
    
    def remove_missing_tabs (self):
        lonely_tab_names_1 = []
        lonely_tab_names_2 = []
        tab_names = self.tab_names_1.keys()
        for tab_name in tab_names:
            if self.tab_names_2.has_key(tab_name) == False:
                lonely_tab_names_1.append(tab_name)
                del self.tab_names_1[tab_name]
        tab_names = self.tab_names_2.keys()
        for tab_name in tab_names:
            if self.tab_names_1.has_key(tab_name) == False:
                loneley_tab_names_2.append(tab_name)
                del self.tab_names_2[tab]
        self.common_tab_names = self.tab_names_1
        if lonely_tab_names_1 != [] or lonely_tab_names_2 != []:
            print '#  Lonely tabs 1', lonely_tab_names_1
            print '#  Lonely tabs 2', lonely_tab_names_2

    def find_tabs (self):
        self.tab_list_1 = self.book1.sheets()
        self.tab_list_2 = self.book2.sheets()
        self.tabs_1 = {}
        self.tabs_2 = {}
        self.tab_names_1 = {}
        self.tab_names_2 = {}
        for tab in self.tab_list_1:
            self.tab_names_1[tab.name] = True
            self.tabs_1[tab.name] = tab
        for tab in self.tab_list_2:
            self.tab_names_2[tab.name] = True
            self.tabs_2[tab.name] = tab
        
    def find_spreadsheet (self, job_dir):
        for f in os.listdir(job_dir):
            if f[-4:] == '.xls' and f[-15:] != 'Description.xls':
                return os.path.join(job_dir, f)
        return None
    
    def open_spreadsheets (self, job_id):
        if self.comparison_level == 'run':
            self.job_dir_1 = os.path.join(self.result_dir_1, job_id)
            self.job_dir_2 = os.path.join(self.result_dir_2, job_id)
            spreadsheet_path_1 = self.find_spreadsheet(self.job_dir_1)
            self.book1 = xlrd.open_workbook(spreadsheet_path_1)
            spreadsheet_path_2 = self.find_spreadsheet(self.job_dir_2)
            self.book2 = xlrd.open_workbook(spreadsheet_path_2)
            
    def compare_a_job (self, job_id):
        print '# Job:', job_id
        self.current_job_id = job_id
        self.open_spreadsheets(job_id)
        self.find_tabs()
        self.remove_missing_tabs()
        self.compare_common_tabs()
        self.close_spreadsheets()
        print '\n\n'
            
    def compare_common_jobs (self):
        for job_id in self.job_ids_1.keys():
            self.compare_a_job(job_id)

    def remove_missing_jobs (self):
        self.jobs_not_in_1 = {}
        self.jobs_not_in_2 = {}
        job_ids = self.job_ids_2.keys()
        for job_id in job_ids:
            if self.job_ids_1.has_key(job_id) == False:
                print job_id, 'not in', self.comp1
                del self.job_ids_2[job_id]
        job_ids = self.job_ids_1.keys()
        for job_id in job_ids:
            if self.job_ids_2.has_key(job_id) == False:
                print job_id, 'not in', self.comp2
                del self.job_ids_1[job_id]

    def find_jobs (self):
        self.job_ids_1 = {}
        self.job_ids_2 = {}
        if self.comparison_level == 'run':
            for d in os.listdir(self.result_dir_1):
                if os.path.isdir(os.path.join(self.result_dir_1, d)):
                    self.job_ids_1[d] = True
            for d in os.listdir(self.result_dir_2):
                if os.path.isdir(os.path.join(self.result_dir_2, d)):
                    self.job_ids_2[d] = True
        elif self.comparison_level == 'job':
            self.job_ids_1[self.comp1] = True
            self.job_ids_2[self.comp2] = True

    def run (self):
        self.find_jobs()
        self.remove_missing_jobs()
        self.compare_common_jobs()

if __name__ == '__main__':

    comparison_level = sys.argv[1]
    comp1 = sys.argv[2]
    comp2 = sys.argv[3]
    v = VerifyTest(comparison_level, comp1, comp2)
    
    v.run()