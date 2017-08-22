import os
import sys

working_dir = 'd:\\cravat\\new driver set'

aas = ['A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'Y']

cancer_genes = []
f = open(os.path.join(working_dir, 'cancerGenes.txt'))
for line in f:
    cancer_genes.append(line.strip())
f.close()

transcript_to_gene = {}
cancer_mutations = {}
wf = open(os.path.join(working_dir, 'driver_set.txt'), 'w')
wf2 = open(os.path.join(working_dir, 'driver_set_detail.txt'), 'w')
f = open(os.path.join(working_dir, 'CosmicMutantExport_v65_200513.tsv'))
for line in f:
    toks = line.strip().split('\t')
    gene = toks[0]
    if gene in cancer_genes:
        mutation_description = toks[15].split(' ')[-1]
        if mutation_description == 'Missense':
            transcript = toks[1]
            aa_change = toks[14].split('.')[1]
            
            if cancer_mutations.has_key(transcript) == False:
                cancer_mutations[transcript] = {}
            
            ref_aa = aa_change[0]
            alt_aa = aa_change[-1]
            try:
                pos = int(aa_change[1:-1])
            except ValueError:
                print aa_change
                continue
                
            if ref_aa in aas and alt_aa in aas:
                if cancer_mutations[transcript].has_key(pos) == False:
                    cancer_mutations[transcript][pos] = {}
                    
                if cancer_mutations[transcript][pos].has_key(aa_change) == False:
                    cancer_mutations[transcript][pos][aa_change] = 1
                    transcript_to_gene[transcript] = gene
f.close()

transcripts = cancer_mutations.keys()
transcripts.sort()

count = 0
for transcript in transcripts:
    poss = cancer_mutations[transcript].keys()
    poss.sort()
    
    for pos in poss:
        for aa_change in cancer_mutations[transcript][pos].keys():
            wf.write(transcript + '\t' + aa_change + '\n')                        
            wf2.write(str(count) + '\t' + transcript + '\t' + aa_change + '\t' + transcript_to_gene[transcript] + '\n')
            count += 1

f = open('d:\\cravat\\new driver set\\drivers.tmps.tp53')
for line in f:
    toks = line.strip().split('\t')
    transcript = toks[0]
    aa_change = toks[1]
    wf.write(transcript + '\t' + aa_change + '\n')
    wf2.write(str(count) + '\t' + transcript + '\t' + aa_change + '\tTP53\n')
wf.close()
wf2.close()

print len(cancer_mutations), 'genes in cancer_mutations'