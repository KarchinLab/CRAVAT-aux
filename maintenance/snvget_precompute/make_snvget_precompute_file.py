import os
import sys

f=open('chrs_with_arff.txt')
datasets = [chrom.strip() for chrom in f.readlines() if chrom[0] != '#']
f.close()

for dataset in datasets:
    print dataset
    
    f = open(os.path.join(dataset, dataset + '.forCravat.txt'))
    variants = {}
    for line in f:
        line = line[:-1]
        try:
            [uid, chrom, pos, strand, ref, alt]  = line.split('\t')
        except:
            print '  Error line:', line
        variants[uid] = [chrom.lower(), pos, strand, ref, alt]
    f.close()
    print '  variants loaded'

    wf = open(dataset + '.snvgetprecompute.txt', 'w')
    f = open(os.path.join(dataset, dataset + '.arff'))
    continuation = False
    columns = []
    count = 0
    for line in f:
        line = line.strip()
        if line == '':
            continue
        if line[0] == '@':
            continue

        count += 1
        if count % 1000000 == 0:
            print ' ', count, 'lines processed'

        toks = line.split(' ')
        
        if continuation == False:
            uid = toks[0]
            accession_aachange_toks = toks[1].split('_')
            accession = '_'.join(accession_aachange_toks[:-1])
            aachange = accession_aachange_toks[-1]
            if toks[-1] == '&':
                continuation = True
                toks = toks[:-1]
            else:
                continuation = False
            columns = toks[2:]
        else:
            if toks[-1] == '&':
                continuation = True
                toks = toks[:-1]
            else:
                continuation = False
            columns.extend(toks)

        if continuation == False:
            [chrom, pos, strand, ref, alt] = variants[uid]
            wf.write('\t'.join([chrom, pos, ref, alt, accession, aachange]) + '\t' + '\t'.join(columns) + '\n')
            columns = []
            uid = None
            accession = None
            aachange = None
    f.close()
    wf.close()
