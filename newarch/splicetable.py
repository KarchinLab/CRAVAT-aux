'''
This script needs splicetable.txt made by snvbox build script.
'''

def get_bin (start):
    BIN_OFFSETS = [512 + 64 + 8 + 1, 64 + 8 + 1, 8 + 1, 1, 0]
    SHIFT_FIRST = 17
    start_bin = start
    start_bin >>= SHIFT_FIRST
    offset = BIN_OFFSETS[0]
    start_bin = offset + start_bin
    return start_bin

def main ():
    f = open('splicetable.txt')
    wf = open('splicetable.newarch.txt', 'w')
    for line in f:
        [chrom, gpos, tid, strand, apos] = line[:-1].split('\t')
        binno = get_bin(int(gpos))
        wf.write('\t'.join([chrom, str(binno), gpos, tid, apos]) + '\n')
    f.close()
    wf.close()

if __name__ ==  '__main__':
    main()