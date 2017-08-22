import sys

input_filename = sys.argv[1]

f = open(input_filename)
mutations = {}
for line in f:
    toks = line.split('\t')
    mutation = toks[1]+':'+toks[2]+':'+toks[4]+':'+toks[5]+':'+toks[6]
    if mutations.has_key(mutation) == False:
        mutations[mutation] = 1
        print line.strip()