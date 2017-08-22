f = open('C:\\Users\\Rick\\Downloads\\vest_precompute_test_dev_correctexon\\Variant_Analysis.Result.tsv')
for line in f:
    if line[0] == '#':
        continue
    else:
        toks = line.split('\t')
        vest_col_no = toks.index('All transcripts and functional scores')
        break

match_count = 0
mismatch_count = 0

for line in f:
    toks = line.split('\t')
    [precomputed_score, precomputed_transcript, dummy] = toks[0].split(';')
    vest_col = toks[vest_col_no]
    vest_transcript_strs = vest_col.split(',')
    match = False
    for vest_transcript_str in vest_transcript_strs:
        [server_transcript, fornext] = vest_transcript_str.split(':')
        [server_aachange, fornext] = fornext.split('(')
        server_transcript += ':' + server_aachange
        server_score = fornext.split(')')[0]
        if server_transcript == precomputed_transcript:
            print server_score, precomputed_score
            if server_score == precomputed_score:
                match = True
                break
            else:
                #print precomputed_transcript, '->', precomputed_score, '  vs  ', server_transcript, '->', server_score, '             ', ' '.join(toks[1:3])
                break
    if match:
        match_count += 1
        #print toks
    else:
        mismatch_count += 1
        #print toks

print 'match:', match_count
print 'mismatch:', mismatch_count
