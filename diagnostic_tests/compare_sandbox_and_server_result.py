import sys
sys.path.append('d:\\CRAVAT\\CRAVAT\\WebContent\\Wrappers\\lib\\python2.5\\site-packages')
import email
import imaplib
import os
import shutil
import time
import urllib
import xlrd
import xlwt
import zipfile

class BookComparison:
    def __init__ (self, comp_book, ref_book, input_coordinate, analysis_type, job_id_in_comparison, wf):
        self.comp_book = comp_book
        self.ref_book = ref_book
        self.input_coordinate = input_coordinate
        self.analysis_type = analysis_type
        self.job_id_in_comparison = job_id_in_comparison
        self.wf = wf
        self.has_same_number_of_tabs = False
        self.compare_tabs()
        
    def compare_tabs (self):
        self.comp_sheets = self.comp_book.sheets()
        self.ref_sheets = self.ref_book.sheets()
        if len(self.comp_sheets) == 0:
            self.wf.write(self.job_id_in_comparison + ' {No tab in the result spreadsheet}\n')
            return
        elif len(self.comp_sheets) != len(self.ref_sheets):
            self.has_same_number_of_tabs = False
            self.wf.write(self.job_id_in_comparison + ' {Different number of tabs}\n')
            return
        else:
            self.has_same_number_of_tabs = True
            for sheet_no in xrange(len(self.comp_sheets)):
                self.compare_sheet(sheet_no)
    
    def compare_sheet (self, sheet_no):
        # comp is sadnbox, ref is server.
        comp_sheet = self.comp_sheets[sheet_no]
        sheet_name = comp_sheet.name
        if sheet_name == 'SNVBox':
            ref_sheet = self.ref_book.sheet_by_name('SnvGet')
        else:
            ref_sheet = self.ref_book.sheet_by_name(sheet_name)
        nrows_comp = comp_sheet.nrows
        nrows_ref = ref_sheet.nrows
        header_row_comp = None
        header_row_ref = None
        for row in xrange(nrows_comp):
            if comp_sheet.cell_value(row, 0) in ['ID', 'Transcript', 'HUGO symbol']:
                header_row_comp = row
                break
        for row in xrange(nrows_ref):
            if ref_sheet.cell_value(row, 0) in ['ID', 'Transcript', 'HUGO symbol']:
                header_row_ref = row
                break
#        print sheet_name, header_row_comp, header_row_ref
        if header_row_comp == None or header_row_comp == nrows_comp - 1:
            self.wf.write(self.job_id_in_comparison + ' <' + sheet_name + '> {No data rows}\n')
            return
        else:
            header_row_dif_ref_comp = header_row_ref - header_row_comp
            headers_comp = []
            headers_ref = []
            headers_common = []
            ncols_comp = comp_sheet.ncols
            ncols_ref = ref_sheet.ncols
            for col in xrange(ncols_comp):
                headers_comp.append(comp_sheet.cell_value(header_row_comp, col))
            for col in xrange(ncols_ref):
                headers_ref.append(ref_sheet.cell_value(header_row_ref, col))
            for header in headers_comp:
                if not header in headers_ref:
                    if not header in ['Best CHASM score', 'CHASM', 'Best VEST score', 'VEST']:
                        self.wf.write(self.job_id_in_comparison + ' <' + sheet_name + '> [' + header + '] New column\n')
                    else:
                        headers_common.append(header)
                else:
                    headers_common.append(header)
            for header in headers_ref:
                if not header in headers_comp:
                    if not header in ['Best CHASM score', 'CHASM', 'Best VEST score', 'VEST']:
                        self.wf.write(self.job_id_in_comparison + ' <' + sheet_name + '> [' + header + '] Absent column\n')
            row_ids_comp = {}
            row_ids_ref = {}
            row_ids_common = []
            for row_comp in xrange(header_row_comp + 1, nrows_comp):
                row_ids_comp[comp_sheet.cell_value(row_comp, 0)] = row_comp
            for row_ref in xrange(header_row_ref + 1, nrows_ref):
                row_ids_ref[ref_sheet.cell_value(row_ref, 0)] = row_ref
            for uid in row_ids_comp.keys():
                if row_ids_ref.has_key(uid) == False:
                    self.wf.write(self.job_id_in_comparison + ' <' + sheet_name + '> [' + uid + '] New UID\n')
            for uid in row_ids_ref.keys():
                if row_ids_comp.has_key(uid) == False:
                    self.wf.write(self.job_id_in_comparison + ' <' + sheet_name + '> [' + uid + '] Absent UID\n')
                else:
                    row_ids_common.append(uid)
            for uid in row_ids_common:
                row_comp = row_ids_comp[uid]
                row_ref = row_ids_ref[uid]
                for header in headers_common:
                    col_comp = headers_comp.index(header)
                    if header == 'Best CHASM score':
                        header = 'CHASM'
                    elif header == 'Best VEST score':
                        header = 'VEST'
                    col_ref = headers_ref.index(header)
#                    row_ref = row_comp + header_row_dif_ref_comp
#                    print 'row_ref=',row_ref,'col_ref=',col_ref
                    comp_value = comp_sheet.cell_value(row_comp, col_comp)
                    ref_value = ref_sheet.cell_value(row_ref, col_ref)
                    if comp_value != ref_value:
                        self.wf.write(self.job_id_in_comparison + ' <' + sheet_name + '> [' + header + '] [' + uid + '] { (' + str(row_comp) + ',' + str(col_comp) + ') ' + str(comp_value) + ' vs ' + '(' + str(row_ref) + ',' + str(col_ref) + ') ' + str(ref_value) + ' (ref)}\n')

class BookCheck:
    def __init__ (self, book, input_coordinate, hg18_option, analysis_type, job_ids, job_id, wf_book):
        self.book = book
        self.input_coordinate = input_coordinate
        self.hg18_option = hg18_option
        self.analysis_type = analysis_type
        self.job_ids = job_ids
        self.job_id = job_id
        self.output_book = wf_book
        self.variant_sheet_name = 'Variant Analysis'
        self.snvget_sheet_name = 'SnvGet'
        self.gene_level_sheet_name = 'Gene Level Analysis'
        self.headers_by_sheet_name = {}
        self.check_sheets()
                    
class VerifyTest:
    first_data_row_map = {'chasm':11, 'vest':9, 'snvget':11}
    data_column_map = {'genomic':{'Variant Analysis':11, 'SnvGet':[11, 96]}, \
                       'transcript':{'Variant Analysis':6, 'SnvGet':[6, 91]}}
    columns_to_ignore = ['ID', \
                         'Chromosome', \
                         'Position', \
                         'Strand', \
                         'Reference base', \
                         'Alternate base', \
                         'Sample ID']
    def __init__ (self):
        self.test_definition_filename = 'd:\\cravat\\cravat_system_test\\test_definition.txt'
        self.cravat_analysis_result_store_folder = 'd:\\cravat\\cravat_system_test\\result'
        self.input_comparison_map = {}
        self.test_definition = {}
        (source_dir, dummy) = os.path.split(__file__)
        self.source_dir = source_dir
        self.headers_by_sheet_name = {}
        self.frac_row_with_result_by_job_sheet_header = {}
        
    def get_job_data (self, basename):
        analysis_type = None
        if basename.count('driver.analysis') == 1:
            analysis_type = 'driver'
        elif basename.count('functional.analysis') == 1:
            analysis_type = 'functional'
        elif basename.count('geneannotationonly.analysis') == 1:
            analysis_type = 'geneannotationonly'
        input_coordinate = None
        if basename.count('genomic') == 1:
            input_coordinate = 'genomic'
        elif basename.count('transcript') == 1:
            input_coordinate = 'transcript'
        hg18_option = basename.split('_')[1]
        input_basename = '.'.join(basename.split('.')[:2])
        return [input_basename, input_coordinate, hg18_option, analysis_type]
    
    def initialize_comparison_data (self, log_filename):
        self.job_ids = []
        self.job_id_map = {}
        self.input_comparison_map = {}
        job_submission_log_filename = os.path.join(self.source_dir, log_filename)
        f = open(job_submission_log_filename)
        for line in f:
            if line[0] != '#':
                toks = line.rstrip().split(' ')
                input_basename = toks[0]
                analysis_type = toks[1]
                job_id = toks[2]
                self.job_ids.append(job_id)
                self.job_id_map[job_id] = (input_basename, analysis_type)
                if self.input_comparison_map.has_key(input_basename) == False:
                    self.input_comparison_map[input_basename] = {}
                self.input_comparison_map[input_basename][analysis_type] = None
        f.close()
    
    def fetch_result_files (self):
        os.chdir(self.cravat_analysis_result_store_folder)
        conn = imaplib.IMAP4_SSL('imap.gmail.com', 993)
        conn.login('cravattest', 'royalenfield')
        conn.select()
        self.no_email_job_ids = []
        self.error_job_ids = []
        for job_id in self.job_ids:
            print ' ', job_id
            response, email_id = conn.search(None, 'Body', job_id)
            if email_id == ['']:
                self.wf.write(job_id + ' {No email}\n')
                print job_id, 'No email'
                self.no_email_job_ids.append(job_id)
            else:
                email_id = email_id[0]
                response, content = conn.fetch(email_id, '(RFC822)')
                content = content[0][1]
                if content.count('Please download your CRAVAT analysis result at') == 1:
                    lines = content[content.index('Please download your CRAVAT analysis result at'):].split('\n')
                    result_file_url = lines[2].strip()
                    result_filename = os.path.join(self.cravat_analysis_result_store_folder, \
                                                   os.path.basename(result_file_url))
                    urllib.urlretrieve(result_file_url, result_filename)
                    z = zipfile.ZipFile(result_filename)
                    xls_file_flag = False
                    for contained_filename in z.namelist():
                        (dirname, basename) = os.path.split(contained_filename)
                        if os.path.exists(dirname) == False:
                            os.mkdir(dirname)
                        wf = open(contained_filename, 'wb')
                        wf.write(z.read(contained_filename))
                        wf.close()
                        if contained_filename[-4:] == '.xls':
                            if contained_filename != 'SnvGet Feature Description.xls':
                                xls_file_flag = True
                    z.close()
                    if xls_file_flag == False:
                        self.wf.write(job_id + ' {No result spreadsheet}\n')
                    os.remove(result_filename)
                else:
                    self.wf.write(job_id + 'Error\n')
                    print '  ' + job_id + ' Error'
                    self.error_job_ids.append(job_id)
    
    def get_xls_filename (self, dirname):
        filenames = os.listdir(dirname)
        for filename in filenames:
            if filename[-4:] == '.xls':
                if filename != 'SnvGet Feature Description.xls':
                    return filename
        return None
    
    def compare_result_files (self):
        server_spreadsheet_filenames = {}
        filenames = os.listdir(self.cravat_analysis_result_store_folder)
        for filename in filenames:
            if filename[-3:] == 'xls':
                server_spreadsheet_filenames[filename] = 1
        sandbox_spreadsheet_filenames = {}
        filenames = os.listdir(self.sandbox_result_directory)
        for filename in filenames:
            if filename[-3:] == 'xls':
                sandbox_spreadsheet_filenames[filename] = 1
#        for filename in server_spreadsheet_filenames:
#            if sandbox_spreadsheet_filenames.has_key(filename) == False:
#                print 'popping sandbox', filename
#                server_spreadsheet_filenames.pop(filename)
#        for filename in sandbox_spreadsheet_filenames:
#            if server_spreadsheet_filenames.has_key(filename) == False:
#                print 'popping server', filename
#                sandbox_spreadsheet_filenames.pop(filename)
        self.wf.write('# Start of comparison\n')
        for filename in server_spreadsheet_filenames:
            server_spreadsheet_filename = \
                os.path.join(self.cravat_analysis_result_store_folder, \
                             filename)
            sandbox_spreadsheet_filename = \
                os.path.join(self.sandbox_result_directory, \
                             'sandbox_'+filename)
            print '    ' + filename
            [input_basename, input_coordinate, hg18_option, analysis_type] = \
                self.get_job_data(filename)
            result_xls_filename = sandbox_spreadsheet_filename
            reference_xls_filename = server_spreadsheet_filename
            result_book = xlrd.open_workbook(result_xls_filename)
            reference_book = xlrd.open_workbook(reference_xls_filename)
            comparison = BookComparison(result_book, \
                                        reference_book, \
                                        input_coordinate, \
                                        analysis_type, \
                                        filename, \
                                        self.wf)
            self.input_comparison_map[input_basename][analysis_type] = comparison
        self.wf.write('# End of comparison')

    def check_sheets (self, result_book, input_coordinate, hg18_option, analysis_type, job_id):
        if self.frac_row_with_result_by_job_sheet_header.has_key(job_id) == False:
            self.frac_row_with_result_by_job_sheet_header[job_id] = {}
        for sheet in result_book.sheets():
            nrows = sheet.nrows
            header_row = None
            for row in xrange(nrows):
                if sheet.cell_value(row, 0) in ['ID', 'Transcript', 'HUGO symbol']:
                    header_row = row
                    break
            if header_row != None:# and header_row != nrows - 1:
                ncols = sheet.ncols
                sheet_name = sheet.name
                self.frac_row_with_result_by_job_sheet_header[job_id][sheet_name] = {}
                if self.headers_by_sheet_name.has_key(sheet_name) == False:
                    self.headers_by_sheet_name[sheet_name] = []
                col_to_header_map = {}
                for col in xrange(ncols):
                    header = sheet.cell_value(header_row, col)
                    col_to_header_map[col] = header
                    if not header in self.columns_to_ignore:
                        if self.headers_by_sheet_name[sheet_name].count(header) == 0:
                            self.headers_by_sheet_name[sheet_name].append(header)
                total_row_no = nrows - header_row - 1
                for col in col_to_header_map.keys():
                    header = col_to_header_map[col]
                    no_row_with_result = 0
                    for row in xrange(header_row + 1, nrows):
                        value = sheet.cell_value(row, col)
                        xf_index = sheet.cell(row, col).xf_index
                        # xf_index 25 is hyperlink (it seems).
                        if (xf_index == 25 or xf_index == 24) or (value != '' and value != 'No result' and value != 'No GeneCards annotation' and value != 'No search result'):
                            no_row_with_result += 1
                    frac_row_with_result = float(no_row_with_result) / float(total_row_no)
                    self.frac_row_with_result_by_job_sheet_header[job_id][sheet_name][header] = frac_row_with_result

    def write_check_result_output_book (self):
        header_row = 0
        for sheet_name in self.headers_by_sheet_name.keys():
            sheet = self.wf_book.add_sheet(sheet_name)
            headers = self.headers_by_sheet_name[sheet_name]
            for job_id_order in xrange(len(self.job_ids)):
                job_id = self.job_ids[job_id_order]
                row = job_id_order + 1
                sheet.write(row, 0, job_id, self.cell_style)
                (input_basename, analysis_type) = self.job_id_map[job_id]
                sheet.write(row, 1, input_basename, self.cell_style)
                sheet.write(row, 2, analysis_type, self.cell_style)
            sheet.write(0,0,'Job ID', self.cell_style)
            sheet.write(0,1,'Input file name', self.cell_style)
            sheet.write(0,2,'Analysis type', self.cell_style)
            sheet.col(0).width = 30*256
            sheet.col(1).width = 30*256
            sheet.col(2).width = 20*256
            for header_no in xrange(len(headers)):
                header_col = header_no + 3
                header = headers[header_no]
                sheet.write(header_row, header_col, header, self.cell_style)
                for job_id_order in xrange(len(self.job_ids)):
                    job_id = self.job_ids[job_id_order]
                    row = job_id_order + 1
                    if job_id in self.no_email_job_ids:
                        value = 'No email'
                    else:
                        if self.frac_row_with_result_by_job_sheet_header[job_id].has_key(sheet_name):
                            if self.frac_row_with_result_by_job_sheet_header[job_id][sheet_name].has_key(header):
                                value = self.frac_row_with_result_by_job_sheet_header[job_id][sheet_name][header]
                            else:
                                value = ''
                        else:
                            value = ''
                    try:
                        value = float('%.1f'%(float(value)*100.0))
                        if value < 50.0:
                            sheet.write(row, header_col, value, self.bad_cell_style)
                        else:
                            sheet.write(row, header_col, value, self.cell_style)
                    except Exception, e:
                        value = str(value)
                        sheet.write(row, header_col, value, self.cell_style)
                
    def check_result_output (self):
        first_job_row = 1
        job_id_col = 1
        result_email_col = 0
        wf_check_sheet = self.wf_book.add_sheet('Result email', self.cell_style)
        wf_check_sheet.write(0,0,'Result email returned?', self.cell_style)
        wf_check_sheet.write(0,1,'Job ID', self.cell_style)
        wf_check_sheet.col(result_email_col).width = 30*256
        wf_check_sheet.col(job_id_col).width = 30*256
        for job_order in xrange(len(self.job_ids)):
            job_id = self.job_ids[job_order]
            if job_id in self.no_email_job_ids:
                print ' ', job_id, 'had no result email - skipped'
                continue
            if job_id in self.error_job_ids:
                print ' ', job_id, 'had an error - skipped'
                continue
            print ' ', job_id
            job_id_row = job_order + first_job_row
            wf_check_sheet.write(job_id_row, job_id_col, job_id, self.cell_style)
            if job_id in self.no_email_job_ids:
                wf_check_sheet.write(job_id_row, result_email_col, 'Fail', self.cell_style)
            else:
                wf_check_sheet.write(job_id_row, result_email_col, 'OK', self.cell_style)
                result_dirname = os.path.join(self.cravat_analysis_result_store_folder, job_id)
                result_xls_basename = self.get_xls_filename(result_dirname)
                [input_basename, input_coordinate, hg18_option, analysis_type] = \
                    self.get_job_data(result_xls_basename)
                result_book = xlrd.open_workbook(os.path.join(result_dirname, \
                                                              result_xls_basename), \
                                                 formatting_info=True)
                self.check_sheets(result_book, \
                                  input_coordinate, \
                                  hg18_option, \
                                  analysis_type, \
                                  job_id)

    def clear_result_directory (self):
        if os.path.exists(self.cravat_analysis_result_store_folder) == False:
            os.mkdir(self.cravat_analysis_result_store_folder)
        for filename in os.listdir(self.cravat_analysis_result_store_folder):
            file_path = os.path.join(self.cravat_analysis_result_store_folder, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    def extract_result_spreadsheets (self):
        cwd = os.getcwd()
        folders = os.listdir(self.cravat_analysis_result_store_folder)
        for folder in folders:
            if os.path.isdir(os.path.join(self.cravat_analysis_result_store_folder, folder)):
                os.chdir(os.path.join(self.cravat_analysis_result_store_folder, folder))
                filenames = os.listdir('.')
                for filename in filenames:
                    if filename[-3:] == 'xls' and filename[:6] != 'SnvGet':
                        shutil.copy(filename, self.cravat_analysis_result_store_folder)
        folders = os.listdir(self.sandbox_result_directory)
        for folder in folders:
            if os.path.isdir(os.path.join(self.sandbox_result_directory, folder)):
                os.chdir(os.path.join(self.sandbox_result_directory, folder))
                filenames = os.listdir('.')
                for filename in filenames:
                    if filename[-3:] == 'xls' and filename[:6] != 'SnvGet':
                        shutil.copy(filename, \
                                    os.path.join(self.sandbox_result_directory, \
                                                 'sandbox_'+filename))
        os.chdir(cwd)

    def open_output_files (self):
        self.wf = open(os.path.join(self.source_dir, 'comparison_result.txt'), 'w')
        self.wf_book = xlwt.Workbook()
        self.cell_style = xlwt.easyxf('align: wrap 1, horiz center')
        self.cell_style.num_format_str = '0.0'
        self.bad_cell_style = xlwt.easyxf('align: wrap 1, horiz center; pattern: pattern solid, fore-colour 2')
#        alignment = xlwt.Alignment()
#        alignment.wrap = xlwt.Alignment.WRAP_AT_RIGHT
#        self.cell_style.alignment = alignment
        
    def close_output_files (self):
        self.wf.close()
        self.wf_book.save(os.path.join(self.source_dir, 'output_check.xls'))
#        self.wf_book.save('d:\\output_check.xls')
        
    def run (self, server_submission_log_filename):
        self.cravat_analysis_result_store_folder = \
            os.path.join('d:\\cravat\\cravat_system_test', server_submission_log_filename)
        print 'Open output files'
        self.open_output_files()
        print 'Clear result directory'
        self.clear_result_directory()
        print 'Initialize comparison data'
        self.initialize_comparison_data(server_submission_log_filename)
        print 'Fetch result files'
        self.fetch_result_files()
        print 'Extract result spreadsheets'
        self.extract_result_spreadsheets()
        self.no_email_job_ids = []
        self.error_job_ids = []
        print 'Compare result files'
        self.compare_result_files()
        print 'Check result output'
        self.check_result_output()
        print 'Write check result output book'
        self.write_check_result_output_book()
        print 'Close output files'
        self.close_output_files()
        self.wf.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: python compare_sandbox_and_server_result.py server_submission_log_filename'
        sys.exit(1)
    v = VerifyTest()
    v.sandbox_result_directory = 'd:\\cravat\\cravat_system_test\\sandbox_result'
    server_submission_log_filename = sys.argv[1]
    v.run(server_submission_log_filename)