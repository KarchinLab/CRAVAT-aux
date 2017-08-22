import os
import sys

class TCGA:
#     tcga_dir = '/databases/tumor-mutations/data/maf/2012-03-13'
    tcga_dir = '/projects/pancan_atlas/hotmaps_public/data/mutations/public_mc3'
    data_paths_by_tissue = {\
        'BLCA' : ['broad.mit.edu_BLCA.IlluminaGA_DNASeq.Level_2.0.0.0'],\
        'BRCA' : ['genome.wustl.edu_BRCA.IlluminaGA_DNASeq.Level_2.3.2.0'],\
        'CESC' : ['broad.mit.edu_CESC.IlluminaGA_DNASeq.Level_2.1.0.0'],\
        'COAD' : ['hgsc.bcm.edu_COAD.IlluminaGA_DNASeq.Level_2.1.1.0',\
                  'hgsc.bcm.edu_COAD.SOLiD_DNASeq.Level_2.1.1.0'],\
        'GBM' :  ['broad.mit.edu_GBM.ABI.public.maf',\
                  'broad.mit.edu_GBM.ABI.1.32.0',\
                  'hgsc.bcm.edu_GBM.ABI.2.7.0',\
                  'hgsc.bcm.edu_GBM.ABI.public.maf',\
                  'genome.wustl.edu_GBM.ABI.53.12.0',\
                  'genome.wustl.edu_GBM.ABI.public.maf'],\
        'KIRC' : ['broad.mit.edu_KIRC.IlluminaGA_DNASeq.Level_2.0.1.0'],\
        'LAML' : ['genome.wustl.edu_LAML.IlluminaGA_DNASeq.Level_2.2.2.0'],\
        'LUAD' : ['broad.mit.edu_LUAD.IlluminaGA_DNASeq.Level_2.0.0.0'],\
        'LUSC' : ['broad.mit.edu_LUSC.IlluminaGA_DNASeq.Level_2.0.0.0'],\
        'OV' :   ['hgsc.bcm.edu_OV.SOLiD_DNASeq.Level_3.1.maf',\
                  'genome.wustl.edu_OV.IlluminaGA_DNASeq.Level_3.1.maf',\
                  'broad.mit.edu_OV.IlluminaGA_DNASeq.Level_3.7.maf',\
                  'hgsc.bcm.edu_OV.ABI.1.2.0',\
                  'genome.wustl.edu_OV.ABI.26.0.0'],\
        'READ' : ['hgsc.bcm.edu_READ.SOLiD_DNASeq.Level_2.1.1.0',\
                  'hgsc.bcm.edu_READ.IlluminaGA_DNASeq.Level_2.1.2.0'],\
        'UCEC' : ['genome.wustl.edu_UCEC.IlluminaGA_DNASeq.Level_2.1.1.0']\
    }
    headers_to_use = [header.upper() for header in \
                      ['Hugo_Symbol', 'Entrez_Gene_Id', \
                       'Center', 'GSC_Center', \
                       'NCBI_Build', \
                       'Chromosome', 'Start_position', 'End_position', \
                       'Strand', \
                       'Variant_Classification', 'Variant_Type', \
                       'Reference_Allele', 'Reference_Alleles', \
                       'Tumor_Seq_Allele1', 'Tumor_Seq_Allele2', \
                       'dbSNP_RS', 'dbSNP_Val_Status', \
                       'Tumor_Sample_Barcode', \
                       'Matched_Norm_Sample_Barcode', \
                       'Match_Norm_Sample_Barcode', \
                       'Match_Normal_Sample_Barcode', \
                       'Match_Norm_Seq_Allele1', 'Match_Norm_Seq_Allele2', \
                       'Tumor_Validation_Allele1', 'Tumor_Validation_Allele2', \
                       'Verification_Status', \
                       'Validation_Method', 'Validation_Status', \
                       'Mutation_Status', 'Assay Mutation_Status', \
                       'Gene_List']]
    samples_to_ignore = {'BLCA':[], \
                         'BRCA':[], \
                         'CESC':['TCGA-DR-A0ZM-01A-12D-A10S-08'], \
                         'COAD':['TCGA-AA-3864-01A-01W-0995-10', \
                                 'TCGA-AA-3977-01A-01W-0995-10', \
                                 'TCGA-AA-3984-01A-02W-0995-10', \
                                 'TCGA-AA-A00N-01A-02W-A00E-09', \
                                 'TCGA-AA-A010-01A-01W-A00E-09'], \
                         'GBM':[], \
                         'KIRC':['TCGA-AS-3778-01A-01D-0966-08'], \
                         'LAML':['TCGA-AB-2833-03B-01W-0728-08', \
                                 'TCGA-AB-2806-03B-01W-0728-08', \
                                 'TCGA-AB-2826-03B-01W-0728-08'], \
                         'LUAD':['TCGA-17-Z031-01A-01W-0746-08', \
                                 'TCGA-05-4396-01A-21D-1855-08'], \
                         'LUSC':['TCGA-18-3409-01A-01D-0983-08'], \
                         'OV':[], \
                         'READ':['TCGA-AG-A02N-01A-11W-A096-10', \
                                 'TCGA-AG-3892-01A-01W-1073-09', \
                                 'TCGA-AG-A002-01A-01W-A005-10', \
                                 'TCGA-AG-A002-01A-01W-A00K-09'], \
                         'UCEC':['TCGA-AX-A0J1-01A-11W-A062-09', \
                                 'TCGA-D1-A17Q-01A-11D-A12J-09', \
                                 'TCGA-D1-A175-01A-11D-A12J-09', \
                                 'TCGA-AX-A05Z-01A-11W-A027-09', \
                                 'TCGA-D1-A103-01A-11D-A10M-09', \
                                 'TCGA-AP-A051-01A-21W-A027-09', \
                                 'TCGA-AX-A0J0-01A-11D-A117-09', \
                                 'TCGA-AX-A06F-01A-11W-A027-09', \
                                 'TCGA-AP-A056-01A-11W-A027-09', \
                                 'TCGA-BS-A0UF-01A-11D-A10B-09', \
                                 'TCGA-BS-A0UV-01A-11D-A10B-09', \
                                 'TCGA-A5-A0G1-01A-11D-A122-09', \
                                 'TCGA-B5-A11E-01A-11D-A10M-09', \
                                 'TCGA-B5-A0JY-01A-11D-A10B-09', \
                                 'TCGA-AP-A059-01A-21D-A122-09', \
                                 'TCGA-AP-A0LM-01A-11D-A122-09', \
                                 'TCGA-A5-A0G2-01A-11W-A062-09']}
    min_no_toks = len(headers_to_use)

    def __init__ (self):
        pass

    def check_available_options (self):
        variant_types = {}
        mutation_statuses = {}
        variant_classifications = {}
        validation_statuses = {}
        mutations_by_group = {}
        for variant_type in ['SNP', 'NON_SNP']:
            mutations_by_group[variant_type] = {}
            for mutation_status in ['SOMATIC', 'GERMLINE', 'VARIANT']:
                mutations_by_group[variant_type][mutation_status] = {}
                for variant_classification in ['MISSENSE', 'OTHERS']:
                    mutations_by_group[variant_type][mutation_status][variant_classification] = {}
                    for validation_status in ['VALID', 'UNKNOWN', 'WILDTYPE']:
                        mutations_by_group[variant_type][mutation_status][variant_classification][validation_status] = []
        unique_mutations_by_tissue = get_mutations_from_all_tissues()
        for tissue, mutations in unique_mutations_by_tissue.items():
            for mutation in mutations:
                mut_variant_type = mutation['variant_type']
                mut_mutation_status = mutation['mutation_status']
                mut_variant_classification = mutation['variant_classification']
                mut_validation_status = mutation['validation_status']
                if variant_types.has_key(mut_variant_type) == False:
                    variant_types[mut_variant_type] = 1
                if mutation_statuses.has_key(mut_mutation_status) == False:
                    mutation_statuses[mut_mutation_status] = 1
                if variant_classifications.has_key(mut_variant_classification) == False:
                    variant_classifications[mut_variant_classification] = 1
                if validation_statuses.has_key(mut_validation_status) == False:
                    validation_statuses[mut_validation_status] = 1
        print '\tvariant_types:',variant_types.keys()
        print '\tmutation_statuses',mutation_statuses.keys()
        print '\tvariant_classifications',variant_classifications.keys()
        print '\tvalidation_statuses',validation_statuses.keys()

    def check_duplicate_mutations (self):
        self.get_maf_file_paths()
        tissues = self.maf_file_paths_by_tissue.keys()
        unique_mutations_by_tissue = {}
        for tissue in tissues:
            print tissue,':',
            records_in_tissue = self.read_tissue(tissue, True)
            print '\tTotal number of mutations for', tissue, '=', len(records_in_tissue.keys())
            self.check_duplicates(records_in_tissue)

    def check_duplicates (self, records_in_tissue):
        chosen_mutations = {}
        for mutation_str in records_in_tissue.keys():
            chosen_mutation = None
            records = records_in_tissue[mutation_str]
            if len(records) == 1:
                (maf_file_path, mutation, line) = records[0]
                chosen_mutation = mutation
            else:
                print '\t',mutation_str
                reason_for_duplicate_found_flag = False
                reasons = []
                for header in records[0][1].keys():
                    values_for_header = []
                    lines = []
                    file_paths = []
                    for [file_path, mutation, line] in records:
                        value = mutation[header]
                        if not value in values_for_header:
                            values_for_header.append(value)
                        lines.append(line)
                        file_paths.append(file_path)
                    if len(values_for_header) > 1:
                        reasons.append('multiple '+header)
                        print '\t\t@Multiple', header
                        print '\t\t\t#', values_for_header
                        for i in xrange(len(lines)):
                            print '\t\t\t#',lines[i],'(',file_paths[i],')'
                        reason_for_duplicate_found_flag = True
                if reason_for_duplicate_found_flag == False:
                    file_paths = []
                    lines = []
                    for [file_path, mutation, line] in records:
                        if not file_path in file_paths:
                            file_paths.append(file_path)
                        if not line in lines:
                            lines.append(line)
                    if len(file_paths) > 1:
                        reasons.append('same mutation in multiple files')
                        print '\t\t@Same mutation in multiple files'
                        for file_path in file_paths:
                            print '\t\t\t#', file_path
                        reason_for_duplicate_found_flag = True
                    elif len(lines) == 1:
                        reasons.append('same line in multiple lines')
                        print '\t\t@Same line in multiple lines'
                        print '\t\t\t#', lines[0], '(', file_paths[0], ')'
                        reason_for_duplicate_found_flag = True
                if reason_for_duplicate_found_flag == False:
                    reasons.append('other reason')
                    print '\t\t@Other reason'
                    print '\t\t\t# lines=',lines
                    line_same_flag = True
                    ref_line = ''
                    ref_file_path = ''
                    for [file_path, mutation, line] in records:
                        print '\t\t\t#',line.split('\t')
                        if ref_line == '':
                            ref_line = line
                            ref_file_path = file_path
                        else:
                            if line != ref_line:
                                line_same_flag = False
                                print '\t\t\t# line difference'
                                print '\t\t\t\t#', ref_line.split('\t'), ref_file_path
                                print '\t\t\t\t#', line.split('\t'), file_path
                                sys.exit()
                print '\t\t!',';'.join(reasons)
    
    def get_maf_file_paths (self):
        self.maf_file_paths_by_tissue = {}
        for tissue in self.data_paths_by_tissue.keys():
            self.maf_file_paths_by_tissue[tissue] = []
            data_paths = self.data_paths_by_tissue[tissue]
            for data_path in data_paths:
                maf_file_path = ''
                path_to_check = os.path.join(self.tcga_dir, tissue, data_path)
                if os.path.isdir(path_to_check):
                    for file_to_check in os.listdir(path_to_check):
                        if file_to_check[-3:] == 'maf':
                            maf_file_path = os.path.join(path_to_check, file_to_check)
                else:
                    if path_to_check[-3:] == 'maf':
                        maf_file_path = path_to_check
                if maf_file_path != '':
                    self.maf_file_paths_by_tissue[tissue].append(maf_file_path)

    def get_mutations_from_all_tissues (self, verbose_flag):
        self.get_maf_file_paths()
        tissues = self.maf_file_paths_by_tissue.keys()
        unique_mutations_by_tissue = {}
        for tissue in tissues:
            print tissue,':',
            records_in_tissue = self.read_tissue(tissue, verbose_flag)
            print '\tTotal number of mutations for', tissue, '=', len(records_in_tissue.keys())
            unique_mutations = self.pick_unique_mutations(records_in_tissue, verbose_flag)
            unique_mutations_by_tissue[tissue] = unique_mutations
        return unique_mutations_by_tissue

    def pick_unique_mutations (self, records_in_tissue, verbose_flag):
        unique_mutations = []
        for mutation_str in records_in_tissue.keys():
            chosen_mutation = None
            chosen_line = None
            records = records_in_tissue[mutation_str]
            if len(records) == 1:
                (maf_file_path, mutation, line) = records[0]
                chosen_mutation = mutation
            else:
                reason_of_choice = 'NA'
                for record in records:
                    (maf_file_path, mutation, line) = record
                    if chosen_mutation == None:
                        chosen_mutation = mutation
                        chosen_line = line
                        reason_of_choice = 'First'
                    else:
                        if mutation['validation_status'] == 'VALID' and \
                            chosen_mutation['validation_status'] != 'VALID':
                            chosen_mutation = mutation
                            chosen_line = line
                            reason_of_choice = 'VALIDATION'
                        elif mutation['verification_status'] == 'VALID' and \
                            chosen_mutation['verification_status'] != 'VALID':
                            chosen_mutation = mutation
                            chosen_line = line
                            reason_of_choice = 'VERIFICATION'
                        else:
                            if chosen_mutation['validation_status'] == 'VALID' and \
                                mutation['validation_status'] != 'VALID':
                                reason_of_choice = 'VALIDATION'
                            elif chosen_mutation['verification_status'] == 'VALID' and \
                                mutation['verification_status'] != 'VALID':
                                reason_of_choice = 'VERIFICATION'
                if verbose_flag == True:
                    print '@',reason_of_choice
                    print '\t!',chosen_line.split('\t')
                    for record in records:
                        print '\t#',record[2].split('\t')
            unique_mutations.append(chosen_mutation)
        return unique_mutations
    
    def print_tcga_directories (self):
        tissues = [tissue for tissue in os.listdir(self.tcga_dir) \
                   if os.path.isdir(os.path.join(self.tcga_dir, tissue))]
        print tissues
        for tissue in tissues:
            tissue_dir = os.path.join(self.tcga_dir, tissue)
            print '\''+tissue+'\'',':','['+','.join(\
                ['\''+d+'\'' for d in os.listdir(tissue_dir)])+'],'

    def read_maf_line (self, line):
        mutation_1 = None
        mutation_2 = None
        if line[0] != '#':
            toks = line.rstrip().upper().split('\t')
            if len(toks) >= 10:
                if line[:4] == 'Hugo':
                    for header in self.headers_to_use:
                        if header in toks:
                            self.header_column_dict[header] = toks.index(header)
                else:
                    hugo_symbol = toks[self.header_column_dict['HUGO_SYMBOL']]
                    gene_id = toks[self.header_column_dict['ENTREZ_GENE_ID']]
                    try:
                        center = toks[self.header_column_dict['CENTER']]
                    except KeyError:
                        center = toks[self.header_column_dict['GSC_CENTER']]
                    ncbi_build = toks[self.header_column_dict['NCBI_BUILD']]
                    chromosome = toks[self.header_column_dict['CHROMOSOME']]
                    strand = toks[self.header_column_dict['STRAND']]
                    start = toks[self.header_column_dict['START_POSITION']]
                    stop = toks[self.header_column_dict['END_POSITION']]
                    try:
                        refbase = toks[self.header_column_dict['REFERENCE_ALLELE']]
                    except KeyError:
                        refbase = toks[self.header_column_dict['REFERENCE_ALLELES']]
                    variant_classification = toks[self.header_column_dict['VARIANT_CLASSIFICATION']]
                    variant_type = toks[self.header_column_dict['VARIANT_TYPE']]
                    altbase_1 = toks[self.header_column_dict['TUMOR_SEQ_ALLELE1']]
                    altbase_2 = toks[self.header_column_dict['TUMOR_SEQ_ALLELE2']]
                    tumor_sample = toks[self.header_column_dict['TUMOR_SAMPLE_BARCODE']]
                    try:
                        norm_sample = toks[self.header_column_dict['MATCH_NORM_SAMPLE_BARCODE']]
                    except KeyError:
                        try:
                            norm_sample = toks[self.header_column_dict['MATCHED_NORM_SAMPLE_BARCODE']]
                        except KeyError:
                            norm_sample = toks[self.header_column_dict['MATCH_NORMAL_SAMPLE_BARCODE']]
                    match_norm_seq_allele_1 = toks[self.header_column_dict['MATCH_NORM_SEQ_ALLELE1']]
                    match_norm_seq_allele_2 = toks[self.header_column_dict['MATCH_NORM_SEQ_ALLELE2']]
                    tumor_validation_allele_1 = toks[self.header_column_dict['TUMOR_VALIDATION_ALLELE1']]
                    tumor_validation_allele_2 = toks[self.header_column_dict['TUMOR_VALIDATION_ALLELE2']]
                    verification_status = toks[self.header_column_dict['VERIFICATION_STATUS']]
                    try:
                        validation_method = toks[self.header_column_dict['VALIDATION_METHOD']]
                    except Exception, e:
                        validation_method = ''
                    validation_status = toks[self.header_column_dict['VALIDATION_STATUS']]
                    try:
                        mutation_status = toks[self.header_column_dict['MUTATION_STATUS']]
                    except KeyError:
                        try:
                            mutation_status = toks[self.header_column_dict['ASSAY MUTATION_STATUS']]
                        except KeyError:
                            print toks
                            print line
                            print self.header_column_dict
                            sys.exit()
                    try:
                        gene_list = toks[self.header_column_dict['GENE_LIST']]
                    except KeyError:
                        gene_list = ''
                    if altbase_1 == refbase:
                        mutation_1 = None
                    else:
                        mutation_1 = {\
                            'hugo_symbol':hugo_symbol, \
                            'gene_id':gene_id, \
                            'ncbi_build':ncbi_build, \
                            'center':center, \
                            'chromosome':chromosome, \
                            'strand':strand, \
                            'start':start, \
                            'stop':stop, \
                            'refbase':refbase, \
                            'altbase':altbase_1, \
                            'altbase_1':altbase_1, \
                            'altbase_2':altbase_2, \
                            'variant_classification':variant_classification, \
                            'variant_type':variant_type, \
                            'tumor_sample':tumor_sample, \
                            'norm_sample':norm_sample, \
                            'match_norm_seq_allele_1':match_norm_seq_allele_1, \
                            'match_norm_seq_allele_2':match_norm_seq_allele_2, \
                            'tumor_validation_allele_1':tumor_validation_allele_1, \
                            'tumor_validation_allele_2':tumor_validation_allele_2, \
                            'verification_status':verification_status, \
                            'validation_method':validation_method, \
                            'validation_status':validation_status, \
                            'mutation_status':mutation_status, \
                            'gene_list':gene_list}
                    if altbase_2 == refbase:
                        mutation_2 = None
                    else:
                        mutation_2 = {\
                            'hugo_symbol':hugo_symbol, \
                            'gene_id':gene_id, \
                            'ncbi_build':ncbi_build, \
                            'center':center, \
                            'chromosome':chromosome, \
                            'strand':strand, \
                            'start':start, \
                            'stop':stop, \
                            'refbase':refbase, \
                            'altbase':altbase_2, \
                            'altbase_1':altbase_1, \
                            'altbase_2':altbase_2, \
                            'variant_classification':variant_classification, \
                            'variant_type':variant_type, \
                            'tumor_sample':tumor_sample, \
                            'norm_sample':norm_sample, \
                            'match_norm_seq_allele_1':match_norm_seq_allele_1, \
                            'match_norm_seq_allele_2':match_norm_seq_allele_2, \
                            'tumor_validation_allele_1':tumor_validation_allele_1, \
                            'tumor_validation_allele_2':tumor_validation_allele_2, \
                            'verification_status':verification_status, \
                            'validation_method':validation_method, \
                            'validation_status':validation_status, \
                            'mutation_status':mutation_status, \
                            'gene_list':gene_list}
                    if mutation_1 == mutation_2:
                        mutation_2 = None
        return (mutation_1, mutation_2)

    def read_maf_file (self, maf_file_path, verbose_flag):
        if verbose_flag == True:
            print '\t',maf_file_path
        records_in_maf = {}
        self.header_column_dict = {}
        f = open(maf_file_path)
        for line in f:
            (mutation_1, mutation_2) = self.read_maf_line(line)
            for mutation in [mutation_1, mutation_2]:
                if mutation != None:
                    mutation_str = '_'.join(\
                        [mutation[key] for key in \
                            ['ncbi_build', 'chromosome', 'strand', 'start', 'stop', \
                             'refbase', 'altbase', 'tumor_sample']])
                    record = [maf_file_path, mutation, line.rstrip().upper()]
                    if records_in_maf.has_key(mutation_str):
                        records_in_maf[mutation_str].append(record)
                    else:
                        records_in_maf[mutation_str] = [record]
        f.close()
        return records_in_maf

    def read_tissue (self, tissue, verbose_flag):
        mutations_in_tissue = {}
        for maf_file_path in self.maf_file_paths_by_tissue[tissue]:
            mutations_in_maf = self.read_maf_file(maf_file_path, verbose_flag)
            mutation_strs = mutations_in_maf.keys()
            for mutation_str in mutation_strs:
                entries_to_add = mutations_in_maf[mutation_str]
                for entry_to_add in entries_to_add:
                    if mutations_in_tissue.has_key(mutation_str):
                        mutations_in_tissue[mutation_str].append(entry_to_add)
                    else:
                        mutations_in_tissue[mutation_str] = [entry_to_add]
        return mutations_in_tissue

    def read_tissue_summary (self, tissue=None, constraints=None, header='/cygdrive/c/Users/andywong86/Desktop/geman/TCGA.'):
        mutations = []
        tumor_samples = []
        f = open(header+tissue+'.txt')
        for line in f:
            toks = line.rstrip('\n').split('\t')
            mutation = {}
            mutation['ncbi_build'] = toks[0]
            mutation['chromosome'] = toks[1]
            mutation['strand'] = toks[2]
            mutation['start'] = toks[3]
            mutation['stop'] = toks[4]
            mutation['refbase'] = toks[5]
            mutation['altbase'] = toks[6]
            mutation['tumor_sample'] = toks[7]
            mutation['mutation_status'] = toks[8]
            mutation['verification_status'] = toks[9]
            mutation['validation_status'] = toks[10]
            mutation['variant_classification'] = toks[11]
            mutation['variant_type'] = toks[12]
            mutation['center'] = toks[13]
            mutation['hugo_symbol'] = toks[14]
            mutation['gene_id'] = toks[15]
            tumor_sample = mutation['tumor_sample']
            if not tumor_sample in self.samples_to_ignore[tissue]:
                add_flag = True
                if constraints != None:
                    for [key, values] in constraints:
                        if not mutation[key] in values:
                            add_flag = False
                            break
                if add_flag == True:
                    mutations.append(mutation)
                if not tumor_sample in tumor_samples:
                    tumor_samples.append(tumor_sample)
        f.close()
        return (mutations, tumor_samples)
    
    def write_unique_mutations_by_tissue (self):
        unique_mutations_by_tissue = self.get_mutations_from_all_tissues(False)
        tissues = unique_mutations_by_tissue.keys()
        for tissue in tissues:
            print 'tissue=',tissue
            filename = 'TCGA.'+tissue+'.txt', 'wf'
            try:
                wf = open(filename)
            except TypeError:
                print filename
                sys.exit()
            for mutation in unique_mutations_by_tissue[tissue]:
                string_to_write = mutation['ncbi_build']+'\t'+\
                                  mutation['chromosome']+'\t'+\
                                  mutation['strand']+'\t'+\
                                  mutation['start']+'\t'+\
                                  mutation['stop']+'\t'+\
                                  mutation['refbase']+'\t'+\
                                  mutation['altbase']+'\t'+\
                                  mutation['tumor_sample']+'\t'+\
                                  mutation['mutation_status']+'\t'+\
                                  mutation['verification_status']+'\t'+\
                                  mutation['validation_status']+'\t'+\
                                  mutation['variant_classification']+'\t'+\
                                  mutation['variant_type']+'\t'+\
                                  mutation['center']+'\t'+\
                                  mutation['hugo_symbol']+'\t'+\
                                  mutation['gene_id']+'\n'
                wf.write(string_to_write)
            wf.close()

    def write_mutation_file_for_mysql (self):
        wf = open('d:\\mupit\\tcga\\mutations_for_mysql.txt','w')
        tissues = self.data_paths_by_tissue.keys()
#        id_ = 1
        for tissue in tissues:
            (mutations, tumor_samples) = self.read_tissue_summary(tissue=tissue, constraints=[\
                ['variant_classification',['MISSENSE', 'NONSENSE', 'READTHROUGH', 'SPLICE']]])
            for mutation in mutations:
                strand = mutation['strand']
                refbase = mutation['refbase']
                altbase = mutation['altbase']
                if strand == '-':
                    tmp = refbase
                    refbase = altbase
                    altbase = tmp
                    strand = '+'
                wf.write(tissue+'\t'+\
                         'chr'+mutation['chromosome'].upper()+'\t'+\
                         mutation['start']+'\t'+\
                         refbase+'\t'+\
                         altbase+'\t'+\
                         mutation['variant_classification']+'\t'+\
                         mutation['tumor_sample']+'\t'+\
                         mutation['hugo_symbol']+'\n')
#                id_ += 1
        wf.close()
        
    def count_mutated_genes (self, tissue):
        (mutations,tumor_samples) = self.read_tissue_summary(tissue=tissue, constraints=[['mutation_status','SOMATIC']],header='d:\\cravat\\tcga\\TCGA.')
        genes = []
        for mutation in mutations:
            gene = mutation['hugo_symbol']
            if not gene in genes:
                genes.append(gene)
        genes.sort()
        return len(genes)
    
if __name__ == '__main__':
    tcga = TCGA()
    tcga.check_duplicate_mutations()
    tcga.get_mutations_from_all_tissues(True)
    tcga.check_available_options()
    tcga.write_unique_mutations_by_tissue()