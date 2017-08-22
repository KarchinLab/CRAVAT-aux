import MySQLdb
import sys

"""
Transcript_Exon table:
    cStart: 0-based
    cEnd: 1-based
        * cEnd is always greater than cEnd.
    Str: strand

    +-----+------+--------+------+-------+------+---------+---------+
    | UID | Exon | tStart | tEnd | Chr   | Str  | cStart  | cEnd    |
    +-----+------+--------+------+-------+------+---------+---------+
    |   1 |    1 |      0 |  132 | chr12 | -    | 9220303 | 9220435 |
    |   1 |    2 |    132 |  174 | chr12 | -    | 9220778 | 9220820 |
    |   1 |    3 |    174 |  277 | chr12 | -    | 9221335 | 9221438 |
    |   1 |    4 |    277 |  346 | chr12 | -    | 9222340 | 9222409 |
    |   1 |    5 |    346 |  437 | chr12 | -    | 9223083 | 9223174 |
    |   1 |    6 |    437 |  565 | chr12 | -    | 9224954 | 9225082 |
    |   1 |    7 |    565 |  784 | chr12 | -    | 9225248 | 9225467 |
    |   1 |    8 |    784 | 1008 | chr12 | -    | 9227155 | 9227379 |
    |   1 |    9 |   1008 | 1189 | chr12 | -    | 9229351 | 9229532 |
    |   1 |   10 |   1189 | 1264 | chr12 | -    | 9229941 | 9230016 |
    |   1 |   11 |   1264 | 1421 | chr12 | -    | 9230296 | 9230453 |
    |   1 |   12 |   1421 | 1509 | chr12 | -    | 9231839 | 9231927 |
    |   1 |   13 |   1509 | 1686 | chr12 | -    | 9232234 | 9232411 |
    |   1 |   14 |   1686 | 1770 | chr12 | -    | 9232689 | 9232773 |
    |   1 |   15 |   1770 | 1822 | chr12 | -    | 9241795 | 9241847 |
    |   1 |   16 |   1822 | 1944 | chr12 | -    | 9242497 | 9242619 |
    |   1 |   17 |   1944 | 2071 | chr12 | -    | 9242951 | 9243078 |
    |   1 |   18 |   2071 | 2300 | chr12 | -    | 9243796 | 9244025 |
    |   1 |   19 |   2300 | 2415 | chr12 | -    | 9246060 | 9246175 |
    |   1 |   20 |   2415 | 2527 | chr12 | -    | 9247568 | 9247680 |
    |   1 |   21 |   2527 | 2689 | chr12 | -    | 9248134 | 9248296 |
    |   1 |   22 |   2689 | 2839 | chr12 | -    | 9251202 | 9251352 |
    |   1 |   23 |   2839 | 2982 | chr12 | -    | 9251976 | 9252119 |
    |   1 |   24 |   2982 | 3046 | chr12 | -    | 9253739 | 9253803 |
    |   1 |   25 |   3046 | 3274 | chr12 | -    | 9254042 | 9254270 |
    |   1 |   26 |   3274 | 3436 | chr12 | -    | 9256834 | 9256996 |
    |   1 |   27 |   3436 | 3546 | chr12 | -    | 9258831 | 9258941 |
    |   1 |   28 |   3546 | 3661 | chr12 | -    | 9259086 | 9259201 |
    |   1 |   29 |   3661 | 3782 | chr12 | -    | 9260119 | 9260240 |
    |   1 |   30 |   3782 | 3867 | chr12 | -    | 9261916 | 9262001 |
    |   1 |   31 |   3867 | 4036 | chr12 | -    | 9262462 | 9262631 |
    |   1 |   32 |   4036 | 4057 | chr12 | -    | 9262909 | 9262930 |
    |   1 |   33 |   4057 | 4110 | chr12 | -    | 9264754 | 9264807 |
    |   1 |   34 |   4110 | 4270 | chr12 | -    | 9264972 | 9265132 |
    |   1 |   35 |   4270 | 4454 | chr12 | -    | 9265955 | 9266139 |
    |   1 |   36 |   4454 | 4653 | chr12 | -    | 9268359 | 9268558 |
    +-----+------+--------+------+-------+------+---------+---------+

CodonTable table:
    pos1, pos2, pos3: In (+) strand transcripts, pos3 > pos1. In (-) strand transcripts, pos1 > pos3.

    +-------------+-------+-------+---------+---------+---------+-------+
    | mrnaAcc     | codon | chrom | pos1    | pos2    | pos3    | bases |
    +-------------+-------+-------+---------+---------+---------+-------+
    | NM_000014.4 |     1 | chr12 | 9268444 | 9268443 | 9268442 | ATG   |
    +-------------+-------+-------+---------+---------+---------+-------+

"""

class TranscriptExonPatcher:

    def __init__(self):
        self.db=MySQLdb.connect(host='karchin-db01.icm.jhu.edu',db='SNVBox_dev',user='andywong86',passwd='andy_wong_mysql+86')
        self.cursor=self.db.cursor()
        self.db_hg19 = MySQLdb.connect(host='karchin-db01.icm.jhu.edu',db='hg19',user='andywong86',passwd='andy_wong_mysql+86')
        self.cursor_hg19 = self.db_hg19.cursor()

    def main(self):
        #print 'save acc to cds start'
        #self.save_acc_to_cds_start()
        #print 'save 1st amino acid genomic position'
        #self.save_1st_amino_acid_genomic_position()
        #print 'save transcript exon table'
        #elf.save_transcript_exon_table()
        #print 'save transcript table'
        #self.save_transcript_table()
        #print 'make uid to cds start table'
        #self.make_uid_to_cds_start_table()
        print 'save Transcript_Exon table with aa position'
        self.save_transcript_exon_table_with_aa_position()
        #print 'comparing old and new transcript exon tables'
        #self.compare_old_new_transcript_exon_table()
        #print 'creating and loading the new Transcript_Exon table'
        #self.create_and_load_new_transcript_exon_table()
        self.cursor.close()
        self.cursor_hg19.close()
        self.db.close()
        self.db_hg19.close()

    def save_acc_to_cds_start(self):
        wf = open('acc_to_cds_start.txt', 'w')
        for table_name in ['refGene', 'ccdsGene', 'ensGene']:
            self.cursor_hg19.execute('select name, strand, chrom, cdsStart, cdsEnd from '+table_name)
            for row in self.cursor_hg19.fetchall():
                (acc, strand, chrom, cds_start, cds_end) = row
                acc = acc.split('.')[0]
                if strand == '-':
                    cds_start = cds_end - 1
                wf.write(acc + '\t' + chrom + '\t' + str(cds_start) + '\t' + str(cds_end) + '\n')
        wf.close()

    def save_1st_amino_acid_genomic_position(self):
        acc_to_len = {}
        f = open('transcript_table.txt')
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len] = line.rstrip().split('\t')
            if ccds != 'None':
                acc_to_len[ccds] = aa_len
            elif refseq_t != 'None':
                acc_to_len[refseq_t] = aa_len
                acc_to_len[refseq_p] = aa_len
            elif ens_t != 'None':
                acc_to_len[ens_t] = aa_len
                acc_to_len[ens_p] = aa_len
        f.close()

        codon_1s = {}
        last_codons = {}
        mrnaAccs = []
        self.cursor.execute('select * from CodonTable where codon=1')
        results = self.cursor.fetchall()
        no_mrna_accs = len(results)
        count = 0
        for row in results:
            count += 1
            if count % 100 == 0: print ' ', count, 'out of', no_mrna_accs
            mrnaAcc = row[0]
            codon_1s[mrnaAcc] = row
            self.cursor.execute('select max(codon) from CodonTable where mrnaAcc="' + mrnaAcc + '"')
            last_codon = self.cursor.fetchone()[0]
            self.cursor.execute('select pos1, pos2, pos3, bases from CodonTable where codon=' + str(last_codon))
            last_codons[mrnaAcc] = self.cursor.fetchone()
            mrnaAccs.append(mrnaAcc)
        
        mrnaAccs.sort()

        wf = open('codon_table_codon_1.txt', 'w')
        for mrnaAcc in mrnaAccs:
            (dummy, dummy, chrom, pos1_1, pos2_1, pos3_1, bases_1) = codon_1s[mrnaAcc]
            (pos1_last, pos2_last, pos3_last, bases_last) = last_codons[mrnaAcc]
            wf.write(mrnaAcc + '\t' + chrom + '\t' + str(pos1_1) + '\t' + str(pos2_1) + '\t' + str(pos3_1) + '\t' + bases_1 + '\t' +\
                     str(pos1_last) + '\t' + str(pos2_last) + '\t' + str(pos3_last) + '\t' + bases_last + '\n')
        wf.close()

    def save_transcript_exon_table(self):
        self.cursor.execute('select * from Transcript_Exon')
        data = {}
        uids = []
        for row in self.cursor.fetchall():
            (uid, exon, tstart, tend, chrom, strand, cstart, cend) = row
            uid = int(uid)
            exon = int(exon)
            if exon == 0:
                print row
            if data.has_key(uid) == False:
                data[uid] = {}
                uids.append(uid)
            data[uid][exon] = [uid, exon, tstart, tend, chrom, strand, cstart, cend]

        uids.sort()
        wf = open('transcript_exon_table.txt', 'w')
        for uid in uids:
            exons = data[uid].keys()
            exons.sort()
            for exon in exons:
                wf.write('\t'.join([str(item) for item in data[uid][exon]]) + '\n')
        wf.close()

    def save_transcript_table(self):
        self.cursor.execute('select * from Transcript')
        wf = open('transcript_table.txt', 'w')
        for row in self.cursor.fetchall():
            wf.write('\t'.join([str(column) for column in row])+'\n')
        wf.close()

    def make_uid_to_cds_start_table(self):
        """
        CDS start is saved in 0-based coordinates.
        """
        acc_to_uid = {}
        f = open('transcript_table.txt')
        for line in f:
            toks = line.rstrip().split('\t')
            uid = toks[0]
            ccds = toks[1].split('.')[0]
            nm = toks[2].split('.')[0]
            ens = toks[4].split('.')[0]
            if ccds != 'None':
                acc_to_uid[ccds] = uid
            if nm != 'None':
                acc_to_uid[nm] = uid
            if ens != 'None':
                acc_to_uid[ens] = uid
        f.close()

        wf = open('uid_cds_start.txt', 'w')
        f = open('acc_to_cds_start.txt')
        done_uids = {}
        for line in f:
            [acc, chrom, cds_start, cds_end] = line.rstrip().split('\t')
            if acc_to_uid.has_key(acc):
                uid = acc_to_uid[acc]
                wf.write(uid + '\t' + chrom + '\t' + cds_start + '\t' + cds_end + '\n')
                done_uids[uid] = 1
        f.close()

        f = open('codon_table_codon_1.txt')
        additional_add_count = 0
        for line in f:
            [acc, dummy, chrom, pos1, pos2, pos3, bases] = line.rstrip().split('\t')
            acc = acc.split('.')[0]
            if acc_to_uid.has_key(acc):
                uid = acc_to_uid[acc]
                if done_uids.has_key(uid) == False:
                    cds_start = pos1
                    wf.write(uid + '\t' + chrom + '\t' + cds_start + '\n')
                    done_uids[uid] = 1
                    additional_add_count += 1
        f.close()
        print ' ', additional_add_count, 'added additionally from CodonTable. Total # of UIDs with CDS start =', len(done_uids)
        wf.close()

    def save_transcript_exon_table_with_aa_position(self):
        """
        Using all 1-based coordinates
        """
        print ' loading aa_lens'
        uid_to_len = {}
        f = open('transcript_table.txt')
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len] = line.rstrip().split('\t')
            uid = int(uid)
            if aa_len != 'None':
                uid_to_len[uid] = int(aa_len)
        f.close()

        print ' loading uid_cds_start.txt'
        uid_to_cds_start = {}
        f = open('uid_cds_start.txt')
        for line in f:
            [uid, chrom, cds_start] = line.rstrip().split('\t')
            uid = int(uid)
            cds_start = int(cds_start) + 1
            if uid_to_cds_start.has_key(uid) == False:
                uid_to_cds_start[uid] = {}
            uid_to_cds_start[uid][chrom] = cds_start
        f.close()

        print ' loading transcript_exon_table.txt'
        f = open('transcript_exon_table.txt')
        uids = []
        transcript_exon_table = {}
        for line in f:
            [uid, exon, tstart, tend, chrom, strand, cstart, cend] = line.rstrip().split('\t')
            uid = int(uid)
            exon = int(exon)
            cstart = int(cstart) + 1
            cend = int(cend)
            if transcript_exon_table.has_key(uid) == False:
                uids.append(uid)
                transcript_exon_table[uid] = {}
            transcript_exon_table[uid][exon] = {'tstart':tstart, 'tend':tend, 'chrom':chrom, 'strand':strand, 'cstart':cstart, 'cend':cend}
        f.close()

        uids.sort()

        print ' writing new transcript_exon table text'
        wf = open('new_transcript_exon_table.txt', 'w')
        count = 1
        no_cds_start_uid_count = 0
        for uid in uids:
            #if uid != 10855: continue
            #print ' uid =', uid
            count += 1
            strand = transcript_exon_table[uid][1]['strand']
            chrom = transcript_exon_table[uid][1]['chrom']
            exons = transcript_exon_table[uid].keys()

            aa_len = uid_to_len[uid]

            cds_start_not_found = False

            try:
                cds_start = uid_to_cds_start[uid][chrom]
                #print 'cds_start =', cds_start
                if strand == '-':
                    exons.sort(reverse=True)
                    #print 'strand is -. exons sorted reverse. exons=', exons
            except Exception, e:
                #print ' error in getting cds_start for uid', uid, '. chrom=', transcript_exon_table[uid][1]['chrom']
                no_cds_start_uid_count += 1
                cds_start_not_found = True

            if cds_start_not_found == True:
                #print 'cds_start_not_found is True'
                for exon in exons:
                    exon_data = transcript_exon_table[uid][exon]
                    exon_data['aa_start'] = 0
                    exon_data['aa_end'] = 0
            else:
                #print '  cds_start =', cds_start
                aa_pos = 1
                cds_started = False
                for exon in exons:
                    #print 'cds_started=',cds_started
                    exon_data = transcript_exon_table[uid][exon]
                    if cds_start >= exon_data['cstart'] + 1 and cds_start <= exon_data['cend']:
                        #print '  exon =', exon
                        cds_started = True
                        aa_pos_start_of_exon = aa_pos
                        if strand == '+':
                            coding_nt_size_of_exon = exon_data['cend'] - cds_start + 1
                        else:
                            coding_nt_size_of_exon = cds_start - exon_data['cstart'] + 1
                        #print '   exon start =', exon_data['cstart'], ', exon end =', exon_data['cend']
                        coding_nt_size_of_exon_rem_3 = coding_nt_size_of_exon % 3
                        if coding_nt_size_of_exon_rem_3 >= 2:
                            aa_pos_end_of_exon = aa_pos + coding_nt_size_of_exon / 3
                        else:
                            aa_pos_end_of_exon = aa_pos + coding_nt_size_of_exon / 3 - 1
                        exon_data['aa_start'] = aa_pos_start_of_exon
                        if exon_data['aa_start'] > aa_len:
                            exon_data['aa_start'] = 0
                            exon_data['aa_end'] = 0
                        else:
                            exon_data['aa_end'] = aa_pos_end_of_exon
                            if exon_data['aa_end'] > aa_len:
                                exon_data['aa_end'] = aa_len
                        aa_pos = aa_pos_end_of_exon + 1
                        no_nt_carry_from_previous_exon = coding_nt_size_of_exon_rem_3
                        #print '   coding_nt_size_of_exon =', coding_nt_size_of_exon, ', coding_nt_size_of_exon_rem_3 =', coding_nt_size_of_exon_rem_3
                        #print '   aa_pos_end_of_exon =', aa_pos_end_of_exon, ', no_nt_carry_from_previous_exon =', no_nt_carry_from_previous_exon
                        #print '   aa_start =', aa_pos_start_of_exon, ', aa_end =', aa_pos_end_of_exon
                    else:
                        if cds_started == False:
                            exon_data['aa_start'] = 0
                            exon_data['aa_end'] = 0
                        else:
                            aa_pos_start_of_exon = aa_pos
                            coding_nt_size_of_exon = exon_data['cend'] - exon_data['cstart'] + 1 + no_nt_carry_from_previous_exon
                            if no_nt_carry_from_previous_exon >= 2:
                                coding_nt_size_of_exon -= 3
                            coding_nt_size_of_exon_rem_3 = coding_nt_size_of_exon % 3
                            #print '  exon =', exon
                            #print '   exon start =', exon_data['cstart'], ', exon end =', exon_data['cend']
                            #print '   no_nt_carry_from_previous_exon=', no_nt_carry_from_previous_exon
                            #print '   coding_nt_size_of_exon =', coding_nt_size_of_exon, ', coding_nt_size_of_exon_rem_3 =', coding_nt_size_of_exon_rem_3
                            if coding_nt_size_of_exon_rem_3 >= 2:
                                aa_pos_end_of_exon = aa_pos + coding_nt_size_of_exon / 3
                            else:
                                aa_pos_end_of_exon = aa_pos + coding_nt_size_of_exon / 3 - 1
                            #print '   aa_pos_end_of_exon =', aa_pos_end_of_exon
                            exon_data['aa_start'] = aa_pos_start_of_exon
                            if exon_data['aa_start'] > aa_len:
                                exon_data['aa_start'] = 0
                                exon_data['aa_end'] = 0
                            else:
                                exon_data['aa_end'] = aa_pos_end_of_exon
                                if exon_data['aa_end'] > aa_len:
                                    exon_data['aa_end'] = aa_len
                            aa_pos = aa_pos_end_of_exon + 1
                            no_nt_carry_from_previous_exon = coding_nt_size_of_exon_rem_3
                            #print '   new no_nt_carry_from_previous_exon =', no_nt_carry_from_previous_exon
                            #print '   aa_start =', aa_pos_start_of_exon, ', aa_end =', aa_pos_end_of_exon
                    #print exon_data

            exons.sort()

            for exon in exons:
                exon_data = transcript_exon_table[uid][exon]
                wf.write(str(uid) + '\t' +\
                         str(exon) + '\t' +\
                         exon_data['tstart'] + '\t' +\
                         exon_data['tend'] + '\t' +\
                         exon_data['chrom'] + '\t' +\
                         exon_data['strand'] + '\t' +\
                         str(exon_data['cstart']) + '\t' +\
                         str(exon_data['cend']) + '\t' +\
                         str(exon_data['aa_start']) + '\t' +\
                         str(exon_data['aa_end']) + '\n')
        wf.close()
        print ' CDS start not found for', no_cds_start_uid_count, ' UIDs'

    def compare_old_new_transcript_exon_table(self):
        f_old = open('transcript_exon_table.txt')
        f_new = open('new_transcript_exon_table.txt')
        line_no = 1
        while True:
            line_old = f_old.readline()
            line_new = f_new.readline()
            if line_old == '' and line_new == '':
                break
            toks_old = line_old.split('\t')
            toks_new = line_new.split('\t')
            uid_old = toks_old[0]
            exon_old = toks_old[1]
            uid_new = toks_new[0]
            exon_new = toks_new[1]
            if not (uid_old == uid_new and exon_old == exon_new):
                print '# Difference at line', line_no, ':', line_old.strip(), 'vs', line_new.strip()
                sys.exit()
            line_no += 1
        f_old.close()
        f_new.close()

    def create_and_load_new_transcript_exon_table(self):
        print ' dropping Transcript_Exon'
        self.cursor.execute('drop table Transcript_Exon')
        self.db.commit()
        print ' creating Transcript_Exon'
        self.cursor.execute('create table Transcript_Exon (UID int, Exon int, tStart int, tEnd int, Chr varchar(20), Str char(1), cStart int, cEnd int, aaStart int, aaEnd int) engine=innodb')
        self.db.commit()
        print ' loading Transcript_Exon'
        self.cursor.execute('load data local infile "new_transcript_exon_table.txt" into table Transcript_Exon (UID, Exon, tStart, tEnd, Chr, Str, cStart, cEnd, aaStart, aaEnd)')
        self.db.commit()

tep = TranscriptExonPatcher()
tep.main()
