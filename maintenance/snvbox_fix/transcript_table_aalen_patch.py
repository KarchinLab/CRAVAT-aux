import MySQLdb
import os
import sys

class TranscriptTablePatcher:
    def __init__(self):
        self.db = MySQLdb.connect(host='karchin-db01.icm.jhu.edu', user='andywong86', passwd='andy_wong_mysql+86', db='SNVBox_dev')
        self.cursor = self.db.cursor()

    def main(self):
        print 'making new transcript table text file...'
        self.make_new_transcript_table_text_file()

    def make_new_transcript_table_text_file(self):
        mrnaacc_to_len = {}
        f = open('mrnaacc_aalen.txt')
        for line in f:
            [mrnaAcc, aa_len] = line.rstrip().split('\t')
            mrnaacc_to_len[mrnaAcc] = int(aa_len) - 1
        f.close()

        f = open('transcript_table.txt')
        wf = open('new_transcript_table.txt', 'w')

        mismatch_count = 0
        not_in_codontable_count = 0
        unrecoverable_count = 0

        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len] = line.rstrip().split('\t')
            if mrnaacc_to_len.has_key(refseq_t):
                new_aa_len = mrnaacc_to_len[refseq_t]
            elif mrnaacc_to_len.has_key(ccds):
                new_aa_len = mrnaacc_to_len[ccds]
            elif mrnaacc_to_len.has_key(ens_t):
                new_aa_len = mrnaacc_to_len[ens_t]
            else:
                print uid, 'not in CodonTable:', line.rstrip()
                not_in_codontable_count += 1
                if aa_len == 'None':
                    print ' unrecoverable aa_len (None)'
                    unrecoverable_count += 1
                    continue
                else:
                    aa_len = int(aa_len)
                    new_aa_len = aa_len

            if aa_len != 'None':
                aa_len = int(aa_len)

            if aa_len != 'None' and new_aa_len != aa_len:
                print 'aa len mismatch:', new_aa_len, 'vs', aa_len, ':', line.rstrip()
                mismatch_count += 1

            wf.write('\t'.join([uid, ccds, refseq_t, refseq_p, ens_t, ens_p, str(new_aa_len)]) + '\n')
        f.close()
        wf.close()
        print mismatch_count, 'aa_len mismatches'
        print not_in_codontable_count, 'UIDs not in CodonTable'
        print unrecoverable_count, 'UIDs\' aa_len unrecoverable'

ttp = TranscriptTablePatcher()
ttp.main()
