import os
import sys

for chrom in os.listdir('.'):
    if chrom[:3] == 'chr':
        f=open(os.path.join(chrom, 'Variant_Analysis.Result.tsv'))

        # Skips comment and header lines.
        for i in xrange(10):
            f.readline()

        count_ms = 0.0
        count_vest = 0.0

        for line in f:
            toks = line.split('\t')
            so = toks[12]
            vest = toks[17]
            if so == 'MS':
                count_ms += 1.0
                if vest != ' ':
                    count_vest += 1.0
        f.close()

        print chrom+' '+str(count_vest/count_ms)+' '+str(int(count_vest))+' '+str(int(count_ms))
