# For local use by Rick.
# Obtains and shows the number of mutations and samples for each cancer type folder in ICGC data.

import os
import sys

icgc_dir = 'd:\\cravat\\classifier_rebuilding\\icgc'

cancer_dirs = os.listdir(icgc_dir)

for cancer_dir in cancer_dirs:
    filenames = os.listdir(os.path.join(icgc_dir, cancer_dir))
    for filename in filenames:
        if filename[:3]=='ssm':
            icgc_filename = filename
            break

    # Number of mutations and samples
    f = open(os.path.join(icgc_dir, cancer_dir, icgc_filename))
    line = f.readline() # Header line
    toks = line.split('\t')
    donor_column_no = toks.index('Donor ID')
    try:
        consequence_type_column_no = toks.index('Consequence type')
    except ValueError, e:
        print 'consequence_type does not exist.'
        print line
        print toks
        sys.exit()
    if donor_column_no == -1 or consequence_type_column_no == -1:
        print 'column no error. cancer_dir=', cancer_dir, ', line=[', line.strip(), ']'
        sys.exit()
    num_mutation = 0
    samples = {}
    for line in f:
        toks = line.strip().split('\t')
        if toks[consequence_type_column_no] in ['non_synonymous_coding', 'missense', 'stop_gained', 'stop_lost']:
            num_mutation += 1
#        if len(toks) > donor_column_no:
        samples[toks[donor_column_no]] = 1
    f.close()
    num_sample = len(samples)
    
    print cancer_dir, '\t', num_mutation, '\t', num_sample