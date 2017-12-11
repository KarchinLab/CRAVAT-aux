import vcf
import os
import sys
import argparse
import urllib
import gzip
import subprocess
import shutil
import requests
import sqlite3

"""
This script generates a sqlite file containing population allele frequnecies built from the gnomAD
and ExAc projects (from the BROAD institute). The reference data is fairly large. Ensure that your 
system has at least 1 Tb of free space before proceeding. The process has 6 stages:
1) download
    Download the data (generally, gzip compressed vcf files) from the broad institute.
    This stage is not well tested. It was done manually for the initial run. If you want
    to manually download, the data is found at http://gnomad.broadinstitute.org/downloads .
    Download the Exome VCF Files and all Genome VCF Files. 
2) unzip
    Unzip the downloaded archives. This stage is also not well tested, and was run manually.
    The archives are .bgz files, but can be renamed to .gz and unzipped with gunzip.
    
WARNING: Not included in the python script: split the exome vcf file into individual chromosome files.

3) parse
    Parse the vcf files to extract population sample counts (AN) and variant allele counts (AC).
    Exome and genome data is still seperate at this point.
4) combine
    Combine the exome and genome level allele count files to get unified allele frequenceis (AF)
5) liftover
    Lift the combined hg19/GRCh37 to hg38/GRCh38
6) build
    Insert the combined hg38 AF files into a single sqlite database. This step cannot be run in
    parallel. However, modifications to run it in parallel would probably be simple.
    
All stages can be called individually, and are also grouped into local (stages 2-6) and parallel 
(stages 2-5) portions. All stages can be called for a limited list of chromosomes. This is 
helpful when running in parallel, which is recommended.
"""

        
class GnomadWorker():
    
    commit_threshold = 10000
    genome_pops = ['AFR','AMR','ASJ','EAS','FIN','NFE','OTH']
    exome_pops = genome_pops + ['SAS']
    
    def __init__(self, cmd_args):
        # Parse cmd args
        self.cmd_args = cmd_args
        self.main_dir = os.path.abspath(cmd_args.directory)
        
        # Setup directory structure
        self.gz_dir = os.path.join(self.main_dir, 'gz_files')
        self.vcf_dir = os.path.join(self.main_dir, 'vcf_files')
        self.text_dir = os.path.join(self.main_dir, 'text_files')
        self.countdb_dir = os.path.join(self.main_dir, 'countdb')
        self.combined19_dir = os.path.join(self.main_dir, 'combined19')
        self.bed_dir = os.path.join(self.main_dir, 'bed_files')
        self.combined38_dir = os.path.join(self.main_dir, 'combined38')
        if not(os.path.exists(self.bed_dir)): os.makedirs(self.bed_dir)
        if not(os.path.exists(self.gz_dir)): os.makedirs(self.gz_dir)
        if not(os.path.exists(self.vcf_dir)): os.makedirs(self.vcf_dir)
        if not(os.path.exists(self.text_dir)): os.makedirs(self.text_dir)
        if not(os.path.exists(self.combined19_dir)): os.makedirs(self.combined19_dir)
        if not(os.path.exists(self.combined38_dir)): os.makedirs(self.combined38_dir)
        if not(os.path.exists(self.countdb_dir)): os.makedirs(self.countdb_dir)
        
        # Templates are used to ensure paths are correct between stages.
        # There is almost certainly a better way of doing this.
        self.gz_path_template = os.path.join(self.gz_dir, '?.vcf.gz')
        self.vcfg_path_template = os.path.join(self.vcf_dir, '?.vcf')
        self.vcfe_path_template = os.path.join(self.vcf_dir, '?.exome.vcf')
        self.textg_path_template = os.path.join(self.text_dir, '?.genome.txt')
        self.texte_path_template = os.path.join(self.text_dir, '?.exome.txt')
        self.tempdb_path_template = os.path.join(self.countdb_dir, '?.sqlite')
        self.combined19_path_template = os.path.join(self.combined19_dir, '?.txt')
        self.bed19_path_template = os.path.join(self.bed_dir, '?.19.bed')
        self.bederr_path_template = os.path.join(self.bed_dir, '?.19.err')
        self.bed38_path_template = os.path.join(self.bed_dir, '?.38.bed')
        self.combined38_path_template = os.path.join(self.combined38_dir, '?.txt')
        
        ####################### CUSTOMIZE THIS TO YOUR ENVIRONMENT ############################
        self.liftover_executable_path = os.path.join(self.main_dir, 'liftover', 'liftover')
        self.liftover_chain_path = os.path.join(self.main_dir, 'liftover', 'hg19ToHg38.over.chain')
        
        # Setup the list of chromosomes to run
        all_chroms = ['chr%s' %x for x in list(range(1,23))+['X','Y']] # No chrY in genome data
        all_chroms_set = set(all_chroms)
        if cmd_args.include:
            include_chroms = set(cmd_args.include.split(','))
        else:
            include_chroms = set([])
        if cmd_args.exclude:
            exclude_chroms = set(cmd_args.exclude.split(','))
        else:
            exclude_chroms = set([])
        if include_chroms:
            use_chroms = (all_chroms_set & include_chroms) - exclude_chroms
        else:
            use_chroms = all_chroms_set - exclude_chroms
        self.use_chroms = sorted(use_chroms, key=lambda chrom: all_chroms.index(chrom))
        
    def download(self):\
        """
        Download data from broad institute. Not well tested.
        """
        print('Download')
        url_base = 'https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr?.vcf.bgz'
        for chrom in self.use_chroms:
            print('=== %s ===' %chrom)
            bgz_url = url_base.replace('chr?',chrom)
            gz_path = self.gz_path_template.replace('?',chrom)
            get_webfile(bgz_url, gz_path)
        exome_bgz_url = 'https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/exomes/gnomad.exomes.r2.0.2.sites.vcf.bgz'
        exome_gz_path = self.gz_template.replace('?','exome')
        get_webfile(exome_bgz_url, exome_gz_path)
        
    def get_webfile(self, url, path):
        """
        Download a file from a url. Memory safe. Verbose.
        """
        r = requests.get(url, stream=True)
        total_size = int(r.headers.get('content-length', 0)); 
        chunk_size = 32 * 1024
        total_chunks = total_size / chunk_size
        downloaded_size = 0
        with open(path, 'wb') as wf:
            n = 0
            for chunk in r.iter_content(chunk_size): 
                wf.write(chunk)
                n += 1
                if n%1000 == 0:
                    sys.stdout.write('%d/%d chunks\r' %(n, total_chunks))
            print('%d/%d chunks\r' %(n, total_chunks))
            
    def unzip(self):
        """
        Unzip the gzip archives to the vcf directory.
        """
        print('Unzip')
        for chrom in self.use_chroms:
            print('=== %s ===' %chrom)
            gz_path = self.gz_path_template.replace('?',chrom)
            vcf_path = self.vcf_path_template.replace('?',chrom)
            gunzip_file(gz_path, vcf_path)
        exome_gz_path = self.gz_template.replace('?','exome')
        exome_vcf_path = self.vcf_path_template.replace('?','exome')
        gunzip_file(exome_gz_path, exome_vcf_path)
        
    def gunzip_file(self, gz_path, out_path):
        """
        Unzip a gzipped file. Memory safe.
        """
        gz_file = gzip.open(gz_path)
        vcf_file = open(vcf_path,'w')
        shutil.copyfileobj(gz_file, vcf_file)
    
    def parse(self):
        """
        Parse the genome and exome vcf files to sample numbers and allele counts
        """
        print('Parse')
        info_keys = ['AN','AC']
        for pop in self.genome_pops:
            info_keys += ['AN_%s' %pop, 'AC_%s' %pop]
        headers = ['pos','ref','alt']+info_keys
        # Parse the genome filed (split up by chromosome)
        for chrom in self.use_chroms:
            print('=== %s genome ===' %chrom)
            vcf_path = self.vcfg_path_template.replace('?',chrom)
            text_path = self.textg_path_template.replace('?',chrom)
            text_file = open(text_path,'w')
            text_file.write('#'+'\t'.join(['pos','ref','alt']+info_keys)+'\n')
            if chrom == 'chrY': continue # No chrY in genome data, but need empty text file
            vcf_file = open(vcf_path)
            n = 0
            for vcf_entry in vcf.Reader(vcf_file):
                if n%10000 == 0:
                    sys.stdout.write('line %d\r' %n)
                all_wtoks = self.parse_vcf_entry(vcf_entry, info_keys)
                for wtoks in all_wtoks:
                    text_file.write('\t'.join(map(str,wtoks))+'\n')
                n += 1
                # if n >= 1000: break
            print('line %d' %n)
        # Parse the exon data
        info_keys = ['AN','AC']
        for pop in self.exome_pops:
            info_keys += ['AN_%s' %pop, 'AC_%s' %pop]
        for chrom in self.use_chroms:
            print('=== %s exome ===' %chrom)
            vcf_path = self.vcfe_path_template.replace('?',chrom)
            vcf_file = open(vcf_path)
            text_path = self.texte_path_template.replace('?',chrom)
            text_file = open(text_path,'w')
            text_file.write('#'+'\t'.join(['pos','ref','alt']+info_keys)+'\n')
            n = 0
            for vcf_entry in vcf.Reader(vcf_file):
                if n%10000 == 0:
                    sys.stdout.write('line %d\r' %n)
                all_wtoks = self.parse_vcf_entry(vcf_entry, info_keys)
                for wtoks in all_wtoks:
                    text_file.write('\t'.join(map(str,wtoks))+'\n')
                n+=1
                # if n >= 1000: break
            print('line %d' %n)
    
    def parse_vcf_entry(self, vcf_entry, info_keys):
        """
        Parse a vcf entry for genome change and certain info field keys. Return 
        split into alternate alleles.
        """
        all_wtoks = []
        pos = int(vcf_entry.POS)
        ref = str(vcf_entry.REF)
        alts = vcf_entry.ALT
        for alt_index in range(len(alts)):
            alt = str(alts[alt_index])
            corr_pos, corr_ref, corr_alt = self.fix_vcf_variant(pos, ref, alt)
            wtoks = [corr_pos, corr_ref, corr_alt]
            for info_key in info_keys:
                entry = vcf_entry.INFO[info_key]
                if info_key.startswith('AN'):
                    wtoks.append(entry)
                elif info_key.startswith('AC'):
                    wtoks.append(entry[alt_index])
            all_wtoks.append(wtoks)
        return all_wtoks
                    
    def fix_vcf_variant (self, pos, ref, alt):
        """
        Convert a vcf formatted genomic variant into cravat format
        """
        shared_head, ref, alt, shared_tail = self.__trim_sequence(ref, alt)
        if shared_head:
            pos += len(shared_head)
        if len(ref) == 0:
            ref = '-'
        if len(alt) == 0:
            alt = '-'
        return pos, ref, alt
        
    def __trim_sequence(self, a, b):
        """
        Examine two sequences and return a matching head, unmatching center and 
        matching tail.
        """
        head = ''
        tail = ''
        while (a and b and (a[0] == b[0])):
            head += a[0]
            a = a[1:]
            b = b[1:]
        while (a and b and (a[-1] == b[-1])):
                tail = a[-1] + tail
                a = a[:-1]
                b = b[:-1]
        return head, a, b, tail
    
    
    def combine(self):
        """
        Combine exome and genome allele count files to generate allele frequency.
        Use sqlite database to match variants.
        """
        print('Combine')
        for chrom in self.use_chroms:
            # Write both genome and exome counts to a temporary database
            sqlite_path = self.tempdb_path_template.replace('?',chrom)
            if os.path.exists(sqlite_path): os.remove(sqlite_path)
            dbconn = sqlite3.connect(sqlite_path)
            genome_path = self.textg_path_template.replace('?',chrom)
            # Write genome counts to genome table
            self.write_count_table(genome_path,dbconn,'genome','g')
            exome_path = self.texte_path_template.replace('?',chrom)
            # Write exome counts to exome table
            self.write_count_table(exome_path,dbconn,'exome','e')
            combined19_path = self.combined19_path_template.replace('?',chrom)
            combined19_file = open(combined19_path,'w')
            file_headers = ['#pos','ref','alt','AF']+['AF_'+x for x in self.exome_pops]
            combined19_file.write('\t'.join(file_headers)+'\n')
            c = dbconn.cursor()
            # Select genomes with no exome match, and genome/exome match rows
            q = 'select * from genome as g left join exome as e '\
                +'on g.g_pos=e.e_pos and g.g_ref=e.e_ref and g.g_alt=e.e_alt;'
            n = 0
            for wtoks in self._parse_joined_counts(dbconn, q, 'g'):
                combined19_file.write('\t'.join(map(str,wtoks))+'\n')
                n += 1
            print(n)
            # Select exomes with no genome match
            q = 'select * from exome as e left join genome as g '\
            +'on e.e_pos=g.g_pos and e.e_ref=g.g_ref and e.e_alt=g.g_alt '\
            +'where g_pos is null;'
            n = 0
            for wtoks in self._parse_joined_counts(dbconn, q, 'e'):
                combined19_file.write('\t'.join(map(str,wtoks))+'\n')
                n += 1
            print(n)
            dbconn.close()
            combined19_file.close()
    
    def _parse_joined_counts(self, dbconn, q, primary_prefix):
        """
        Parse a query on the joined exome and genome tables to get allele frequencies
        """
        c = dbconn.cursor()
        c.execute(q)
        headers = [x[0] for x in c.description]
        n = 0
        primary_cols = [primary_prefix+'_'+x for x in ['pos','ref','alt']]
        for row in c:
            rd = dict(zip(headers,row))
            wtoks = [rd[x] for x in primary_cols]
            try:
                total_af = self._get_af(rd)
                wtoks.append(total_af)
            except ZeroDivisionError: # No AN in either genome or exome
                continue
            for pop in self.exome_pops:
                try:
                    pop_af = self._get_af(rd, pop.lower())
                    wtoks.append(pop_af)
                except ZeroDivisionError:
                    wtoks.append(0.0)
            yield wtoks
        c.close()
    
    def _get_af(self, rd, pop=None):
        """
        Get the allele frequency from a joined row. Use pop=None to get total AF.
        """
        keys = ['g_ac','e_ac','g_an','e_an']
        if pop:
            pop = pop.lower()
            keys = [x+'_'+pop for x in keys]
        vals = []
        for key in keys:
            try:
                db_val = rd[key]
            except KeyError: # handles genome not having the SAS population
                db_val = 0
            if db_val is None:
                vals.append(0)
            else:
                vals.append(db_val)
        af = float(vals[0]+vals[1])/float(vals[2]+vals[3])
        return af
            
    def write_count_table(self, text_path, dbconn, table_name, col_prefix):
        """
        Write count file to table. Need col_prefix to ensure that genome and 
        exome tables have unique column names.
        """
        f = open(text_path)
        c = dbconn.cursor()
        header_line = f.readline()[1:].strip('\r\n')
        headers = ['%s_%s' %(col_prefix, x.lower()) for x in header_line.split()]
        af_defs = ['%s int' %x for x in headers[3:]]
        q = 'create table %s (%s_pos integer, %s_ref text, %s_alt, %s);' \
            %(table_name, col_prefix, col_prefix, col_prefix, ', '.join(af_defs))
        c.execute(q)
        c.execute('pragma synchronous=OFF')
        n = 0
        for l in f:
            toks = l.strip('\r\n').split('\t')
            corr_toks = []
            for tok in toks:
                if tok:
                    corr_toks.append(tok)
                else:
                    corr_toks.append('null')
            q = 'insert into %s (%s) values (%s, "%s", "%s", %s);' \
                %(table_name, ', '.join(headers), toks[0], toks[1], toks[2], ', '.join(corr_toks[3:]))
            c.execute(q)
            n += 1
            if n%self.commit_threshold == 0:
                dbconn.commit()
        dbconn.commit()
        q = 'create index %s_pos_idx on %s (%s_pos)' %(table_name, table_name, col_prefix)
        c.execute(q)
        dbconn.commit()
        c.execute('pragma synchronous=2')
        c.close()           
            
    def liftover(self):
        """
        Liftover a allele frequency file from hg19 to hg38
        """
        print('Liftover')
        for chrom in self.use_chroms:
            combined19_path = self.combined19_path_template.replace('?',chrom)
            bed19_path = self.bed19_path_template.replace('?',chrom)
            bederr_path = self.bederr_path_template.replace('?',chrom)
            bed38_path = self.bed38_path_template.replace('?',chrom)
            combined38_path = self.combined38_path_template.replace('?',chrom)
            combined19_file = open(combined19_path)
            bed19_file = open(bed19_path,'w')
            combined19_file.readline() # header line
            n = 0
            for l in combined19_file:
                toks = l.strip('\r\n').split('\t')
                wtoks = [chrom, toks[0], str(int(toks[0])+1)]
                wtoks.append('var%s:%s' %(n, ','.join(toks[1:])))
                bed19_file.write('\t'.join(wtoks)+'\n')
                n += 1
            combined19_file.close()
            bed19_file.close()
            liftover_cmd = [self.liftover_executable_path,
                            bed19_path,
                            self.liftover_chain_path,
                            bed38_path,
                            bederr_path]
            print(liftover_cmd)
            print(' '.join(liftover_cmd))
            os.popen(' '.join(liftover_cmd))
            bed38_file = open(bed38_path)
            combined38_file = open(combined38_path,'w')
            headers = ['#pos','ref','alt','AF'] + ['AF_'+x for x in self.exome_pops]
            combined38_file.write('\t'.join(headers)+'\n')
            print('Extracting from Hg38 BED')
            n = 0
            for l in bed38_file:
                if n%10000 == 0:
                    sys.stdout.write('line %d\r' %n)
                toks = l.strip('\r\n').split('\t')
                wtoks = [toks[1]] + toks[3].split(':')[1].split(',')
                combined38_file.write('\t'.join(wtoks)+'\n')
                n += 1
            print('line %d' %n)
            bed38_file.close()
            combined38_file.close()
    
    def build(self):
        """
        Put allele frequency files into the gnomad.sqlite database
        """
        print('build')
        sqlite_path = os.path.join(self.main_dir, 'gnomad.sqlite')
        if os.path.exists(sqlite_path):
            os.remove(sqlite_path)
        dbconn = sqlite3.connect(sqlite_path)
        c = dbconn.cursor()
        for chrom in self.use_chroms:
            print('=== %s ===' %chrom)
            combined38_path = self.combined38_path_template.replace('?',chrom)
            combined38_file = open(combined38_path)
            headerline = combined38_file.readline()[1:].strip('\r\n')
            headers = [x.lower() for x in headerline.split()]
            af_headers = headers[3:]
            af_defs = ['%s real' %x for x in af_headers]
            q = 'create table %s ' %chrom \
                +'(pos integer not null, ref text not null, alt text not null, ' \
                +'%s);' %', '.join(af_defs)
            c.execute(q)
            c.execute('pragma synchronous=0;')
            n = 0
            for l in combined38_file:
                if n%self.commit_threshold == 0:
                    sys.stdout.write('line %d\r' %n)
                    dbconn.commit()
                toks = l.strip('\r\n').split('\t')
                corr_toks = []
                for tok in toks:
                    if tok == '' or tok == '0.0':
                        corr_toks.append('null')
                    else:
                        corr_toks.append(tok)
                af_toks = corr_toks[3:]
                q = 'insert into %s (%s) values (%s, "%s", "%s", %s);' \
                        %(chrom, ', '.join(headers),
                          toks[0],
                          toks[1],
                          toks[2],
                          ', '.join(af_toks))
                c.execute(q)
                n += 1
                # if n >= 10: break
            print('line %d' %n)
            dbconn.commit()
            c.execute('pragma synchronous=2;')
            q = 'create index %s_idx on %s (pos)' %(chrom, chrom)
            print(q)
            c.execute(q)
            dbconn.commit()
        c.close()
        dbconn.close()
        combined38_file.close()
        

if __name__ == '__main__':
    def call_download(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.download()
        
    def call_unzip(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.unzip()
        
    def call_parse(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.parse()
        
    def call_combine(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.combine()
        
    def call_liftover(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.liftover()
    
    def call_build(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.build()    
    
    def call_all(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.download()
        worker.unzip()
        worker.parse()
        worker.build()
        
    def call_local(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.unzip()
        worker.parse()
        worker.combine()
        worker.liftover()
        worker.build()
        
    def call_sge(cmd_args):
        worker = GnomadWorker(cmd_args)
        worker.unzip()
        worker.parse()
        worker.combine()
        worker.liftover()
        
    main_parser = argparse.ArgumentParser()
    main_parser.add_argument('directory',
                             help='directory to work in')
    main_parser.add_argument('-i',
                             dest='include',
                             default='',
                             help='Chromosomes to include. Comma delimited.')
    main_parser.add_argument('-e',
                             dest='exclude',
                             default='',
                             help='Chromosomes to exclude. Comma delimited.')
    subparsers = main_parser.add_subparsers(help='sub-command help')
    download_parser = subparsers.add_parser('download')
    download_parser.set_defaults(func=call_download)
    build_parser = subparsers.add_parser('unzip')
    build_parser.set_defaults(func=call_unzip)
    build_parser = subparsers.add_parser('parse')
    build_parser.set_defaults(func=call_parse)
    build_parser = subparsers.add_parser('combine')
    build_parser.set_defaults(func=call_combine)
    build_parser = subparsers.add_parser('liftover')
    build_parser.set_defaults(func=call_liftover)
    build_parser = subparsers.add_parser('build')
    build_parser.set_defaults(func=call_build)
    build_parser = subparsers.add_parser('all')
    build_parser.set_defaults(func=call_all)
    build_parser = subparsers.add_parser('local')
    build_parser.set_defaults(func=call_local)
    build_parser = subparsers.add_parser('sge')
    build_parser.set_defaults(func=call_sge)
    cmd_args = main_parser.parse_args()
    cmd_args.func(cmd_args)
    