import os

result_file_dir = os.path.join('c:\\', 'Projects', 'CRAVAT', 'Error jobs', 'vest_NA')

f = open(os.path.join(result_file_dir, 'Variant_Additional_Details.Result.tsv'))

for line in f:
    toks = line[:-1].split('\t')
    if toks[0] == 'Input line':
        break

chroms = ['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 
          'chr9', 'chr10', 'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 
          'chr16', 'chr17', 'chr18', 'chr19', 'chr20', 'chr21', 'chr22',
          'chrx', 'chry']
wfs = {}
for chrom in chroms:
    wfs[chrom] = open(os.path.join(result_file_dir, 'vest_file.' + chrom + '.tsv'), 'w')
    
for line in f:
    toks = line[:-1].split('\t')
    chrom = toks[2].lower()
    pos = toks[3]
    refnt = toks[5]
    altnt = toks[6]
    vest_all = toks[22]
    for vest_str in vest_all.split(','):
        [transcript, aachange_score, dummy] = vest_str.split(':')
        if transcript[0] == '*':
            transcript = transcript[1:]
        [aachange, score] = aachange_score.split('(')
        refaa = aachange[0]
        altaa = aachange[-1]
        if refaa.isalpha() and altaa.isalpha():
            wfs[chrom].write('\t'.join([chrom, pos, refnt, altnt, 
                refaa, altaa, score, transcript + ':' + aachange]) + '\n')

f.close()
for chrom in chroms:
    wfs[chrom].close()

print 'Done'

# rkim@karchin-web02: load_vest_precompute_fix.py is the next step, which
# uses the output files of this script directly.