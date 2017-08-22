"""
This script makes context files only for TCGA maf files.

Usage: python make_context_files.py TCGA_MAF_DATE

       TCGA_MAF_DATE: subfolder name under /databases/tumor-mutations/data/maf/
                      For example, 2013-06-23

This script will look at all the subdirectories (tissues) under 
TCGA_MAF_DATE folder, consolidate the maf files in each subdirectory,
generates TISSUE.vars files, TISSUE.hugo.vars files, TISSUE.hugo.strand.vars files,
and TISSUE.context files under data/ directory.
"""

import os
import subprocess
import sys

class MAFFileTCGA:
    column_names = ['Hugo_Symbol', 
                    'NCBI_Build', 
                    'Chromosome', 
                    'Start_position', 
                    'End_position', 
                    'Strand', 
                    'Variant_Classification', 
                    'Variant_Type', 
                    'Reference_Allele', 
                    'Tumor_Seq_Allele1', 
                    'Tumor_Seq_Allele2', 
                    'Tumor_Sample_Barcode', 
                    'Validation_Status', 
                    'Mutation_Status']
    column_nos = {}
    
    def __init__(self, filename):
        self.f = open(filename)
        line = self.f.readline()
        toks = line.strip().split('\t')
        for column_name in self.column_names:
            self.column_nos[column_name] = toks.index(column_name)
    
    def getLine (self):
        line = self.f.readline()
        if line == None or line == '':
            return None
        else:
            return MAFLineTCGA(line, self.column_names, self.column_nos)
    
    def close (self):
        self.f.close()
    
class MAFLineTCGA:
    def __init__(self, line, column_names, column_nos):
        self.column_names = column_names
        self.data = {}
        toks = line.split('\t')
        if len(toks) < 25:
            self.data = None
            return
        if toks[0] == 'Hugo_Symbol': 
            self.data = None
            return
        for column_name in self.column_names:
            self.data[column_name] = toks[column_nos[column_name]]

    def __str__(self):
        s = '\t'.join([self.data[column_name] for column_name in self.column_names])
        return s
    
class FileICGC:
    column_names = ['Gene name', \
                    'Assembly Version', \
                    'Chromosome', \
                    'Chromosome start', \
                    'Chromosome end', \
                    'Chromosome strand', \
                    'Consequence type', \
                    'Mutation ID', 
                    'Reference genome allele', \
                    'Mutation', \
                    'Mutation', \
                    'Specimen ID', \
                    'Validation status',
                    'Mutation_Status']
    column_nos = {}
    
    def __init__ (self, filename):
        self.f = open(filename)
        line = self.f.readline()
        print line.strip()
        toks = line.strip().split('\t')
        for column_name in self.column_names:
            if column_name == 'Chromosome strand' and toks.count(column_name) == 0:
                self.column_nos[column_name] = -1
            elif column_name == 'Mutation_Status':
                self.column_nos[column_name] = -1
            else:
                self.column_nos[column_name] = toks.index(column_name)
    
    def getLine (self):
        line = self.f.readline();
        if line == None or line == '':
            return None
        else:
            data_line = LineICGC(line, self.column_names, self.column_nos)
            if data_line.data == None:
                return None
            else:
                return data_line
    
    def close (self):
        self.f.close()
    
class LineICGC:

    def __init__(self, line, column_names, column_nos):
        self.column_names = column_names
        self.data = {}
        toks = line.split('\t')
        if len(toks) < 10: 
            self.data = None
            return
        if toks[0] == 'Cancer Type': 
            self.data = None
            return
        for column_name in self.column_names:
            if column_name == 'Chromosome strand' and toks.count(column_name) == 0:
                self.data[column_name] = '+'
            else:
                self.data[column_name] = toks[column_nos[column_name]]
        key = 'Chromosome strand'
        if self.data[key] == 1:
            self.data[key] = '+'
        elif self.data[key] == -1:
            self.data[key] = '-'
        key = 'Consequence type'
        if self.data[key] == 'missense':
            self.data[key] = 'Missense'
        elif self.data[key] == 'stop_gained':
            self.data[key] = 'Nonsense'
        elif self.data[key] == 'stop_lost':
            self.data[key] = 'Nonstop'
        elif self.data[key] == 'non_synonymous_coding':
            self.data[key] = 'Missense'
        key = 'Mutation ID'
        self.data[key] = 'SNP'
        key = 'Reference genome allele'
        if self.data[key].count('/') > 0:
            self.data[key] = self.data[key].split('/')[0]
        key = 'Mutation'
        if self.data[key].count('>') > 0:
            ref_alt = self.data[key].split('>')
            self.data['Reference genome allele'] = ref_alt[0]
            self.data[key] = ref_alt[1]
        elif len(self.data[key]) == 2:
            for base in self.data[key]:
                if base != self.data['Reference genome allele']:
                    self.data[key] = base
                    break
        self.data['Mutation_Status'] = 'Somatic'

    def __str__(self):
        s = '\t'.join([self.data[column_name] for column_name in self.column_names])
        return s

def step1_make_vars (definition_filename):

    print 'Making .vars files'

    f = open(definition_filename)
    for definition_line in f:
        if definition_line[0] == '#':
            continue

        print '  ', definition_line

        toks = definition_line.strip().split('\t')
        print toks
        
        classifier_name = toks[0]
        data_dir = toks[1]
        data_filenames = toks[2].split(',')
        data_type = toks[3]

        vars_filename = os.path.join('data', classifier_name.upper() + '.vars')

        wf = file(vars_filename, 'w')

        for data_filename in data_filenames:
            data_path = os.path.join(data_dir, data_filename)
            if data_type == 'TCGA':
                maf = MAFFileTCGA(data_path)
                wf.write('\t'.join(maf.column_names) + '\n')
            elif data_type == 'ICGC':
                maf = FileICGC(data_path)
                if maf != None:
                    wf.write('\t'.join(maf.column_names) + '\n')
            while True:
                data_line = maf.getLine()
                if data_line == None:
                    break
                else:
                    wf.write(str(data_line) + '\n')
            maf.close()
        
        wf.close()

def step2_make_hugo_vars (definition_filename):

    print 'Making .hugo.vars files'

    f = open(definition_filename)
    for definition_line in f:
        if definition_line[0] == '#':
            continue

        print '  ', definition_line

        toks = definition_line.strip().split('\t')
        print toks
        
        classifier_name = toks[0]
        data_dir = toks[1]
        data_filenames = toks[2].split(',')
        data_type = toks[3]

        print ' '.join(['./src/getHugoForEnsembl', 'data/'+classifier_name+'.vars', 'data/ensToHugo'])
        out = subprocess.check_output(['./src/getHugoForEnsembl', 'data/'+classifier_name+'.vars', 'data/ensToHugo'])

        lines = out.split('\n')

        wf = open('data/' + classifier_name + '.hugo.vars', 'w')
        for line in lines:
            if line.strip() != '':
                wf.write(line+'\n')
        wf.close()

def step3_make_hugo_strand_vars (definition_filename):

    print 'Making .hugo.strand.vars files'

    f = open(definition_filename)
    for definition_line in f:
        if definition_line[0] == '#':
            continue

        print '  ', definition_line

        toks = definition_line.strip().split('\t')
        print toks
        
        classifier_name = toks[0]
        data_dir = toks[1]
        data_filenames = toks[2].split(',')
        data_type = toks[3]

        out = subprocess.check_output(['./src/getGeneStrand', 'data/'+classifier_name+'.hugo.vars', 'data/hugoStrandDict'])

        lines = out.split('\n')

        wf = open('data/' + classifier_name + '.hugo.strand.vars', 'w')
        wf_missense = open(os.path.join('data', classifier_name + '.total.missense.list'), 'w')
        for line in lines:
            if line.strip() != '':
                if data_type == 'TCGA':
                    wf.write(line+'\n')
                elif data_type == 'ICGC':
                    wf.write(line+'\n')
                toks = line.split('\t')
                consequence_type = toks[6]
                if consequence_type.upper().count('MISSENSE') > 0:
                    specimen_id = toks[11]
                    chromosome = toks[2]
                    start = toks[3]
                    end = toks[4]
                    strand = toks[5]
                    ref = toks[8]
                    alt = toks[9]
                    wf_missense.write('\t'.join([specimen_id, chromosome, start, end, strand, ref, alt])+'\n')
        wf.close()

def step4_make_context_files (definition_filename):

    print 'Making .context files'

    f = open(definition_filename)
    for definition_line in f:
        if definition_line[0] == '#':
            continue

        print '  ', definition_line

        toks = definition_line.strip().split('\t')
        print toks
        
        classifier_name = toks[0]
        data_dir = toks[1]
        data_filenames = toks[2].split(',')
        data_type = toks[3]

        out = subprocess.check_output(['./src/makeContextTbl', '-f', 'data/'+classifier_name+'.hugo.strand.vars', '-r'], stderr=subprocess.STDOUT)

        lines = out.split('\n')

        wf = open('data/' + classifier_name + '.context', 'w')
        for line in lines:
            if line.strip() != '':
                wf.write(line+'\n')
        wf.close()

data_definition_filename = sys.argv[1]

#step1_make_vars(data_definition_filename)
#step2_make_hugo_vars(data_definition_filename)
step3_make_hugo_strand_vars(data_definition_filename)
#step4_make_context_files(data_definition_filename)