import os
import random
import scipy.stats
from tcga import TCGA

class MutationTest:
    """
    MutationTest is a class for calculating, from the TCGA data, 
    the empirical p-value of a gene's having as many mutations 
    as those found in the TCGA data by chance. "Background" genes, 
    which are all human genes minus the A- and B-list genes, 
    are used to estimate "by chance" mutation occurrences.
    
    Usage example: 
        # Imports this module.
        >import geman 
        # Initiates the class and loads the TCGA data for the tissue.
        >m=geman.MutationTest(tissue='LUAD') 
        # Runs the p-value calculation for the A-list genes
        >m.run(method='signtest', T=m.a_list, name='a_list')  
    
    Change the value of self.data_folder accordingly.
    
    When initialized, a MutationTest object has the following variables already set:
        a_list: List of HUGO symbols of the A-list genes
        b_list: List of HUGO symbols of the B-list genes
        B: List of HUGO symbols of the background genes that are not in the A-list nor in the B-list
        G: List of human genes to which a gene length was assigned
        gene_length_by_hugo: Dictionary with HUGO symbols and keys and gene lengths (in AA) as values
        bins: List of gene length bins. Each bin is a list [gene length lower end, gene length upper end].
        G_ls_a_list: Dictionary with gene length bin numbers as keys. For each gene length bin number,
                     the value is a list of HUGO symbols of the A-list genes in the gene length bin.
        G_ls_b_list: Dictionary with gene length bin numbers as keys. For each gene length bin number,
                     the value is a list of HUGO symbols of the B-list genes in the gene length bin.
        G_ls_B: Dictionary with gene length bin numbers as keys. For each gene length bin number,
                the value is a list of HUGO symbols of the background genes in the gene length bin.
                     
         
    To add new p-value calculation methods, modify "run" method and make additional class methods.
    "run" method has a common, straightforward calculation procedure. When adding new p-value 
    calculation schemes, following this calculation procedure structure is recommended.
    """
    def __init__ (self, tissue='READ', R=10):
        self.data_folder = os.path.join('d:\\','cravat','geman')
        self.gene_length_lower_limit = 10
        self.gene_length_upper_limit = 3000
        self.R = 100
        self.tcga = TCGA()
        self.tissue = tissue
        self.R = R
        self.make_gene_length_by_hugo()
        self.make_gene_lists()
        self.make_bins(bin_upper_limits=[55, 67, 82, 100, 123, \
                                         150, 183, 224, 274, 335, \
                                         409, 500, 611, 747, 913, \
                                         1116, 1364, 1667, 2037, \
                                         2490, 3043])
        self.make_G_ls()
        print 'tissue=',tissue,', R=',R
    
    def run(self, T=['TP53'], method=None, name='TP53'):
        """
        Use this method as the launch point for different p-value
        calculation methods.
        """
        self.method = method
        self.name = name
        if T == None:
            gene_name_list = self.a_list
        else:
            gene_name_list = T
        self.load_N_g_s_for_tissue(tissue=self.tissue)
        if method == 'ranksum':
            self.make_T(gene_name_list=gene_name_list)
            self.make_Trs()
            self.calculate_N_T_by_s()
            self.calculate_N_Tr_by_s_by_r()
            self.calculate_delta_less_than_by_s_by_r()
            self.calculate_delta_greater_than_by_s_by_r()
            self.calculate_eta_minus_by_s()
            self.calculate_eta_plus_by_s()
            self.do_signed_rank_test()
        elif method == 'signtest':
            self.load_eta_plus_fraction_of_background()
            wf = file(os.path.join(self.data_folder,'pvalue_signtest_'+self.tissue+'_'+self.name+'.txt'),'w')
            for gene in gene_name_list:
                self.make_T(gene_name_list=[gene])
                bin_no = self.Tdata[gene]['bin_no']
                self.make_Trs_with_all_background_genes_in_bin(bin_no)
                self.calculate_N_T_by_s()
                self.calculate_N_Tr_by_s_by_r()
                self.calculate_delta_less_than_by_s_by_r()
                self.calculate_delta_greater_than_by_s_by_r()
                self.calculate_eta_plus_fraction()
                self.calculate_signtest_p_value(bin_no)
                self.calculate_N_T_sum_over_s()
                average_N_T = float(self.N_T_sum_over_s) / float(self.no_sample)
                wf.write(str(bin_no)+'\t'+\
                         gene+'\t'+\
                         str(average_N_T)+'\t'+\
                         str(self.R)+'\t'+\
                         str(self.no_sample)+'\t'+
                         str(self.eta_minus)+'\t'+\
                         str(self.eta_plus)+'\t'+\
                         str(self.eta_plus_fraction)+'\t'+\
                         str(self.p_value)+'\n')
                print '%3s\t%10s\t%8.3f\t%5d\t%5d\t%6d\t%6d\t%8.3f\t%8.5f'%(bin_no,gene,average_N_T,self.R,self.no_sample,self.eta_minus,self.eta_plus,self.eta_plus_fraction,self.p_value)
            wf.close()
        elif method == 'signtest_background':
            for bin_no in xrange(len(self.bins)):
                wf = file(os.path.join(self.data_foldor,'eta_plus_fraction_signtestbackground_'+self.tissue+'_bin_'+str(bin_no)+'.txt'),'w')
                gene_name_list = self.G_ls_B[bin_no]
                for gene in gene_name_list:
#                    print 'gene=',gene,'bin_no=',bin_no
                    self.make_T(gene_name_list=[gene],force_B_flag=True)
                    self.make_Trs_with_all_background_genes_in_bin(self.Tdata[gene]['bin_no'])
                    if self.Trs.count([gene]) > 0:
                        self.Trs.remove([gene])
                        del self.Trdata[gene]
                        self.R -= 1
                    self.calculate_N_T_by_s()
                    self.calculate_N_Tr_by_s_by_r()
                    self.calculate_delta_less_than_by_s_by_r()
                    self.calculate_delta_greater_than_by_s_by_r()
                    self.calculate_eta_plus_fraction()
                    self.calculate_N_T_sum_over_s()
                    average_N_T = float(self.N_T_sum_over_s) / float(self.no_sample)
                    wf.write(gene+'\t'+\
                             str(average_N_T)+'\t'+\
                             str(self.R)+'\t'+\
                             str(self.no_sample)+'\t'+
                             str(self.eta_minus)+'\t'+\
                             str(self.eta_plus)+'\t'+\
                             str(self.eta_plus_fraction)+'\n')
                    print '%3s\t%10s\t%8.3f\t%5d\t%5d\t%6d\t%6d\t%8.3f'%(bin_no,gene,average_N_T,self.R,self.no_sample,self.eta_minus,self.eta_plus,self.eta_plus_fraction)
                wf.close()
        elif method == 'simplesum':
            wf = file(os.path.join(self.data_folder,'pvalue_simplesum_'+self.tissue+'_'+self.name+'.txt'),'w')
            for gene in gene_name_list:
                self.make_T(gene_name_list=[gene])
                self.make_Trs_with_all_background_genes_in_bin(self.Tdata[gene]['bin_no'])
                self.calculate_N_T_sum_over_s()
                self.calculate_N_Tr_sum_over_s_by_r()
                self.do_simple_sum_p_value_calculation()
                wf.write(gene+'\t'+str(self.N_T_sum_over_s)+'\t'+str(self.R)+'\t'+str(self.p_value)+'\n')
                print gene, self.N_T_sum_over_s, self.R, self.p_value
            wf.close()
            
        else:
            print 'wrong command'

    def load_eta_plus_fraction_of_background (self):
        self.eta_plus_fraction_of_background_by_bin_no = {}
        for bin_no in xrange(len(self.bins)):
            self.eta_plus_fraction_of_background_by_bin_no[bin_no] = []
            f = open(os.path.join(self.data_folder,'eta_plus_fraction_signtestbackground_'+self.tissue+'_bin_'+str(bin_no)+'.txt'))
            for line in f:
                toks = line.rstrip().split('\t')
                gene = toks[0]
                eta_plus_fraction = float(toks[-1])
                self.eta_plus_fraction_of_background_by_bin_no[bin_no].append(eta_plus_fraction)
            f.close()

    def write_pvalue_for_each_gene (self, genes=['TP53'], name='TP53'):
        wf = open(os.path.join(self.data_folder,'pvalue_output_'+self.tissue+'_'+name+'_'+str(self.R)+'.txt'),'w')
        wf.write('Gene\tAverage number of mutation in sample\tHow many times T has more mutations\tHow many times background has more mutations\tp_value\n')
        for gene in genes:
            self.run(T=[gene])
            if self.p_value == -1:
                self.p_value = '%10s'%('NA')
            else:
                self.p_value = '%10.7f'%(self.p_value)
            average_no_mutation = float(sum([self.N_T_by_s[s] for s in self.tumor_samples]))/float(len(self.tumor_samples))
            signs = self.sign_by_sample.values()
            no_minus_sign = signs.count(-1)
            no_plus_sign = signs.count(1)
            no_ranked_sample = len(self.samples_with_rank)
            rank_sum = self.rank_sum
            wf.write(gene+'\t'+str(average_no_mutation)+'\t'+str(no_ranked_sample)+'\t'+str(rank_sum)+'\t'+str(no_minus_sign)+'\t'+str(no_plus_sign)+'\t'+str(self.p_value)+'\n')
            print '%10s | %8.3f | %3d | %5d | %3d | %3d | %10s' % (gene, average_no_mutation, no_ranked_sample, rank_sum, no_minus_sign, no_plus_sign, self.p_value)
        wf.close()
        
    def make_gene_length_by_hugo (self):
        self.gene_length_by_hugo = {}
        f = open(os.path.join(self.data_folder,'gene_length.txt'))
        for line in f:
            toks = line.strip('\r').split('\t')
            hugo = toks[0]
            gene_length = int(toks[2]) / 3
            if gene_length >= self.gene_length_lower_limit and gene_length <= self.gene_length_upper_limit:
                self.gene_length_by_hugo[hugo] = gene_length
        f.close()

    def make_gene_lists (self):
        self.a_list = []
        self.b_list = []
        self.G = self.gene_length_by_hugo.keys()
        self.B = []
        f = open(os.path.join(self.data_folder,'a and b list.txt'))
        for line in f:
            toks = line.strip('\n').split('\t')
            hugo = toks[0]
            list_type = toks[-1]
#            print 'toks=',toks,'list_type=',list_type
            if hugo in self.G:
                if list_type == 'A':
                    self.a_list.append(hugo)
                elif list_type == 'B':
                    self.b_list.append(hugo)
        f.close()
        for gene in self.G:
            if not gene in self.a_list and not gene in self.b_list:
                self.B.append(gene)
    
    def make_bins (self, bin_upper_limits=None):
        self.bin_upper_limits = bin_upper_limits
        self.bins = []
        self.bins.append([self.gene_length_lower_limit, bin_upper_limits[0]])
        for i in xrange(1, len(self.bin_upper_limits)):
            self.bins.append([self.bins[i-1][1]+1, self.bin_upper_limits[i]])

    def find_bin_no (self, gene_length):
        for bin_no in xrange(len(self.bins)):
            if gene_length >= self.bins[bin_no][0] and gene_length <= self.bins[bin_no][1]:
                return bin_no
        return -1
    
    def make_G_ls (self):
        self.G_ls_a_list = []
        self.G_ls_b_list = []
        self.G_ls_B = []
        for i in xrange(len(self.bins)):
            self.G_ls_a_list.append([])
            self.G_ls_b_list.append([])
            self.G_ls_B.append([])
        for gene in self.G:
            gene_length = self.gene_length_by_hugo[gene]
            bin_no = self.find_bin_no(gene_length)
            if bin_no != -1:
                if gene in self.a_list:
                    self.G_ls_a_list[bin_no].append(gene)
                elif gene in self.b_list:
                    self.G_ls_b_list[bin_no].append(gene)
                elif gene in self.B:
                    self.G_ls_B[bin_no].append(gene)
                
    def make_T (self, gene_name_list=None, force_B_flag=False):
        self.T = []
        self.Tdata = {}
        for gene in gene_name_list:
            if gene in self.G and ((force_B_flag == False and not gene in self.B) or (force_B_flag == True)):
                self.T.append(gene)
                gene_length = self.gene_length_by_hugo[gene]
                bin_no = self.find_bin_no(gene_length)
                self.Tdata[gene] = {'gene_length':gene_length, 'bin_no':bin_no}
        number_of_genes_in_T = len(self.T)
                
    def get_Tr_gene_in_bin (self, bin_no):
        Tr_gene = self.G_ls_B[bin_no][random.randint(0, len(self.G_ls_B[bin_no])-1)]
        return Tr_gene
    
    def make_Trs (self):
        self.Trs = []
        self.Trdata = {}
        for r in xrange(self.R):
            Tr = []
            for gene in self.T:
                bin_no = self.Tdata[gene]['bin_no']
                Tr_gene = self.get_Tr_gene_in_bin(bin_no)
                Tr.append(Tr_gene)
                self.Trdata[Tr_gene] = {'gene_length':self.gene_length_by_hugo[Tr_gene], 'bin_no':bin_no}
            self.Trs.append(Tr)

    def make_Trs_with_all_background_genes_in_bin (self, bin_no):            
        self.R = len(self.G_ls_B[bin_no])
#        print self.R,'genes in the background bin',bin_no
        self.Trs = []
        self.Trdata = {}
        for gene in self.G_ls_B[bin_no]:
            self.Trs.append([gene])
            self.Trdata[gene] = {'gene_length':self.gene_length_by_hugo[gene], 'bin_no':bin_no}
            
    def load_N_g_s_for_tissue (self, tissue=None):
        (mutations, tumor_samples) = self.tcga.read_tissue_summary(tissue=tissue, constraints=[\
            ['variant_classification',['MISSENSE', 'NONSENSE', 'READTHROUGH', 'SPLICE']]])
        self.tumor_samples = tumor_samples
        self.no_sample = len(self.tumor_samples)
        self.N_g_s = {}
        for gene in self.G:
            self.N_g_s[gene] = {}
            for tumor_sample in tumor_samples:
                self.N_g_s[gene][tumor_sample] = 0
        for mutation in mutations:
            hugo = mutation['hugo_symbol']
            tumor_sample = mutation['tumor_sample']
            if self.N_g_s.has_key(hugo):
                self.N_g_s[hugo][tumor_sample] += 1
            
    def calculate_N_T_by_s (self):
        self.N_T_by_s = {}
        for s in self.tumor_samples:
            N_T = 0
            for gene in self.T:
                N_T += self.N_g_s[gene][s]
            self.N_T_by_s[s] = N_T

    def calculate_N_T_sum_over_s (self):
        self.N_T_sum_over_s = 0
        for s in self.tumor_samples:
            for gene in self.T:
                self.N_T_sum_over_s += self.N_g_s[gene][s]

    def calculate_N_Tr_by_s_by_r (self):
        self.N_Tr_by_s_by_r = {}
        for s in self.tumor_samples:
            self.N_Tr_by_s_by_r[s] = {}
            for r in xrange(self.R):
                self.N_Tr_by_s_by_r[s][r] = 0
                for gene in self.Trs[r]:
                    self.N_Tr_by_s_by_r[s][r] += self.N_g_s[gene][s]
    
    def calculate_N_Tr_sum_over_s_by_r (self):
        self.N_Tr_sum_over_s_by_r = {}
        for r in xrange(self.R):
            self.N_Tr_sum_over_s_by_r[r] = 0
            for s in self.tumor_samples:
                for gene in self.Trs[r]:
                    self.N_Tr_sum_over_s_by_r[r] += self.N_g_s[gene][s]
                
    def calculate_delta_less_than_by_s_by_r (self):
        self.delta_less_than_by_s_by_r = {}
        for s in self.tumor_samples:
            self.delta_less_than_by_s_by_r[s] = {}
            N_T_s = self.N_T_by_s[s]
            for r in xrange(self.R):
                N_Tr_s = self.N_Tr_by_s_by_r[s][r]
                if N_T_s < N_Tr_s:
                    delta = 1
                else:
                    delta = 0
                self.delta_less_than_by_s_by_r[s][r] = delta
    
    def calculate_delta_greater_than_by_s_by_r (self):
        self.delta_greater_than_by_s_by_r = {}
        for s in self.tumor_samples:
            self.delta_greater_than_by_s_by_r[s] = {}
            N_T_s = self.N_T_by_s[s]
            for r in xrange(self.R):
                N_Tr_s = self.N_Tr_by_s_by_r[s][r]
                if N_T_s > N_Tr_s:
                    delta = 1
                else:
                    delta = 0
                self.delta_greater_than_by_s_by_r[s][r] = delta
    
    def calculate_eta_minus_by_s (self):
        self.eta_minus_by_s = {}
        for s in self.tumor_samples:
            sum_delta = 0
            for r in xrange(self.R):
                sum_delta += self.delta_less_than_by_s_by_r[s][r]
            eta_minus = float(sum_delta) / float(self.R)
            self.eta_minus_by_s[s] = eta_minus
    
    def calculate_eta_plus_by_s (self):
        self.eta_plus_by_s = {}
        for s in self.tumor_samples:
            sum_delta = 0
            for r in xrange(self.R):
                sum_delta += self.delta_greater_than_by_s_by_r[s][r]
            eta_plus = float(sum_delta) / float(self.R)
            self.eta_plus_by_s[s] = eta_plus
            
    def calculate_eta_plus_by_r (self):
        self.eta_plus_by_r = {}
        no_sample = len(self.tumor_samples)
        for r in xrange(self.R):
            sum_delta = 0
            for s in self.tumor_samples:
                sum_delta += self.delta_greater_than_by_s_by_r[s][r]
            self.eta_plus_by_r[r] = float(sum_dalta) / float(no_sample)

    def calculate_eta_plus_fraction (self):
        self.eta_minus = 0
        self.eta_plus = 0
        for r in xrange(self.R):
            for s in self.tumor_samples:
                self.eta_minus += self.delta_less_than_by_s_by_r[s][r]
                self.eta_plus  += self.delta_greater_than_by_s_by_r[s][r]
        self.eta_plus_fraction = float(self.eta_plus) / float(self.eta_plus + self.eta_minus)

    def calculate_signtest_p_value (self, bin_no):
        no_lose = 0
        for eta_plus_fraction_background in self.eta_plus_fraction_of_background_by_bin_no[bin_no]:
            if self.eta_plus_fraction <= eta_plus_fraction_background:
                no_lose += 1
        self.p_value = float(no_lose) / float(len(self.eta_plus_fraction_of_background_by_bin_no[bin_no]))
        
    def do_signed_rank_test (self):
        self.abs_dif_by_sample = {}
        self.sign_by_sample = {}
        for s in self.tumor_samples:
            eta_minus = self.eta_minus_by_s[s]
            eta_plus =  self.eta_plus_by_s[s]
            if eta_minus == eta_plus:
                sign = 0
            elif eta_minus > eta_plus:
                sign = 1
            else:
                sign = -1
            abs_dif = abs(eta_minus - eta_plus)
            sign_times_abs_dif = sign*abs_dif
            if sign != 0:
                self.abs_dif_by_sample[s] = abs_dif
                self.sign_by_sample[s] = sign
        self.samples_with_rank = self.abs_dif_by_sample.keys()
        for i in xrange(len(self.samples_with_rank)-1):
            for j in xrange(i+1, len(self.samples_with_rank)):
                abs_dif_i = self.abs_dif_by_sample[self.samples_with_rank[i]]
                abs_dif_j = self.abs_dif_by_sample[self.samples_with_rank[j]]
                if abs_dif_j < abs_dif_i:
                    tmp = self.samples_with_rank[i]
                    self.samples_with_rank[i] = self.samples_with_rank[j]
                    self.samples_with_rank[j] = tmp
        self.rank_by_sample = {}
        current_rank = 1
        abs_difs = self.abs_dif_by_sample.values()
        ranks_for_duplicate_abs_difs = {}
        self.sign_times_rank_by_sample = {}
        for s in self.samples_with_rank:
            abs_dif = self.abs_dif_by_sample[s]
            abs_dif_count = abs_difs.count(abs_dif)
            if abs_dif_count == 1:
                self.rank_by_sample[s] = current_rank
            elif abs_dif_count > 1:
                if not ranks_for_duplicate_abs_difs.has_key(abs_dif):
                    ranks_for_duplicate_abs_difs[abs_dif] = float(abs_dif_count*current_rank)/float(abs_dif_count) + float((abs_dif_count)*(abs_dif_count-1))/2.0/float(abs_dif_count)
                self.rank_by_sample[s] = ranks_for_duplicate_abs_difs[abs_dif]
            self.sign_times_rank_by_sample[s] = self.sign_by_sample[s] * self.rank_by_sample[s]
            current_rank += 1
        self.rank_sum = sum(self.sign_times_rank_by_sample.values())
        if len(self.samples_with_rank) == 0:
            self.z_statistic = -1
            self.p_value = -1
        else:
            eta_minuss = [self.eta_minus_by_s[s] for s in self.tumor_samples]
            eta_pluss =  [self.eta_plus_by_s[s]  for s in self.tumor_samples]
            (self.z_statistic, self.p_value) = scipy.stats.wilcoxon(eta_minuss, eta_pluss)
    
    def do_simple_sum_p_value_calculation (self):
        count_more_mutation_in_background_gene = 0
        for r in xrange(self.R):
            if self.N_T_sum_over_s <= self.N_Tr_sum_over_s_by_r[r]:
                count_more_mutation_in_background_gene += 1
        self.p_value = float(count_more_mutation_in_background_gene) / float(self.R)
