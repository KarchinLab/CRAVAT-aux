#f=open('/databases/tumor-mutations/data/raw/samuels/2011/Samuels_Nonsynonymous.txt')
#wf = open('ML.total.missense.list', 'w')
#for line in f:
#    toks = line.split('\t')
#    gene = toks[0]
#    chr_ = toks[1][3:]
#    pos = str(int(toks[4].replace('"','').replace(',','')) + 1)
#    strand = '+'
#    ref = toks[6]
#    alt = toks[7]
#    ref_aa = toks[10]
#    alt_aa = toks[11]
#    if toks[13].upper() == 'NONSYNONYMOUS' and toks[14].upper() == 'HETEROZYGOUS':
#        if ref_aa != alt_aa and ref_aa != '*' and alt_aa != '*':
#            wf.write(gene+'\t'+chr_+'\t'+pos+'\t'+pos+'\t'+strand+'\t'+ref+'\t'+alt+'\n')

f=open('/databases/tumor-mutations/data/raw/vogelstein/2010/MB/S4_Discovery_Mutations.csv')
wf = open('MB.total.missense.list', 'w')
for line in f:
    toks = line.split('\t')
    if toks[9].upper() == 'MISSENSE':
        gene = toks[1]
        posStr = toks[6]
        posList = posStr.split('.')[1].split(':')
        chr_ = posList[0][3:]
        posList = posList[1].split('>')
        pos = posList[0][:-1]
        [ref, alt] = [posList[0][-1], posList[1]]
        strand = '+'
        aaStr = toks[8].split('.')[1]
        ref_aa = aaStr[0]
        alt_aa = aaStr[-1]
        wf.write(gene+'\t'+chr_+'\t'+pos+'\t'+pos+'\t'+strand+'\t'+ref+'\t'+alt+'\n')