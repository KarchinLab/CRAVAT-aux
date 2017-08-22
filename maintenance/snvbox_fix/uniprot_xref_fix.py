import MySQLdb
import os
import sys

class BlastMatch:
    match_fragment_evalue_cutoff = 0.001

    def __init__(self, uid, lines, query_seq_len):
        self.uid = uid

        # Gets UniProt accession.
        for row_no in xrange(len(lines)):
            if len(lines[row_no]) == 0: continue
            if lines[row_no][0] == '>':
                first_hit_header_row = row_no
                self.uniprot_acc = lines[row_no].split('|')[1]
                #print ' uniprot =', uniprot_acc
                break
        lines = lines[first_hit_header_row:]

        # Finds the UniProt sequence length.
        for row_no in xrange(len(lines)):
            if len(lines[row_no]) == 0: continue
            if lines[row_no].count('Length =') == 1:
                sbjct_seq_len = int(lines[row_no].strip().split('=')[1].strip())
                sbjct_seq_len_row_no = row_no
                break
        lines = lines[sbjct_seq_len_row_no + 1:]
        #print ' sbjct_seq_len =', sbjct_seq_len
        
        # Finds each match fragment text body.
        match_fragment_start_row_nos = []
        for row_no in xrange(len(lines)):
            line = lines[row_no]
            if len(line) > 8:
                if line[:8] == ' Score =':
                    match_fragment_start_row_nos.append(row_no)
        
        match_fragment_end_row_nos = []
        for pos in xrange(1, len(match_fragment_start_row_nos)):
            match_fragment_end_row_nos.append(match_fragment_start_row_nos[pos] - 1)
        match_fragment_end_row_nos.append(len(lines))

        self.blast_match_fragments = []
        for pos in xrange(len(match_fragment_start_row_nos)):
            match_fragment = BlastMatchFragment(self.uid, self.uniprot_acc, lines[match_fragment_start_row_nos[pos]:match_fragment_end_row_nos[pos] + 1])
            if match_fragment.evalue <= self.match_fragment_evalue_cutoff:
                self.blast_match_fragments.append(match_fragment)
        
        # Blast result is already sorted by e-value.

        # Combines match fragments.
        self.query_to_sbjct = {}
        #self.used_query_start_end_poss = []
        for match_fragment in self.blast_match_fragments:
            fragment_query_start = match_fragment.query_start_pos
            fragment_query_end = match_fragment.query_end_pos
            #overlap = False
            #for used_query_start_end_pos in self.used_query_start_end_poss:
            #    [used_query_start, used_query_end] = used_query_start_end_pos
            #    if used_query_start <= fragment_query_start <= used_query_end or used_query_start <= fragment_query_end <= used_query_end:
            #        overlap = True
            #        break
            #if overlap == False:
            for query_pos in match_fragment.query_sbjct_match.keys():
                if self.query_to_sbjct.has_key(query_pos) == False:
                    self.query_to_sbjct[query_pos] = match_fragment.query_sbjct_match[query_pos]
        self.query_poss = self.query_to_sbjct.keys()
        self.query_poss.sort()

        self.no_match_aa = len(self.query_poss)

        #print self.uid, self.uniprot_acc, self.no_match_aa
        #print self.uid, self.uniprot_acc, ','.join([str(query_pos)+'~'+str(self.query_to_sbjct[query_pos]) for query_pos in self.query_poss])

    def __repr__(self):
        #return '\n'.join([m.get_stat() for m in self.blast_match_fragments])
        return '\n'.join([str(self.uid) + '\t' +\
                          str(query_pos) + '\t' +\
                          self.uniprot_acc + '\t' +\
                          str(self.query_to_sbjct[query_pos]) + '\t' +\
                          str(self.no_match_aa) \
                          for query_pos in self.query_poss])

class BlastMatchFragment:
    evalue_cutoff = 0.001
    half_scan_size = 3 # 3 AAs on each side of an AA
    mismatch_allowance = 1 # 1 AA mismatch in 2 * half_scan_size + 1 (middle)

    def find_score_evalue(self, lines):
        for row_no in xrange(len(lines)):
            line = lines[row_no]
            if len(line) > 8:
                if line[:8] == ' Score =':
                    self.score = float(line.split('=')[1].split('b')[0])
                    self.evalue = line.split('=')[2].strip()
                    if self.evalue[0] == 'e':
                        self.evalue = '1' + self.evalue
                    self.evalue = float(self.evalue)
                    lines = lines[row_no + 1:]
                    break
        return lines
    
    def find_query_match_sbjct_lines(self, lines):
        for row_no in xrange(len(lines)):
            line = lines[row_no]
            if len(line) > 6:
                if line[:6] == 'Query:':
                    lines = lines[row_no:]
                    break

        query_lines = []
        match_lines = []
        sbjct_lines = []
        query_check = False
        for line in lines:
            if len(line) == 0 and query_check == False: continue
            if line[:6] == 'Query:':
                query_lines.append(line[6:])
                query_check = True
            elif line[:6] == 'Sbjct:':
                sbjct_lines.append(line[6:])
                query_check = False
            else:
                match_lines.append(line[6:])
        
        no_align_lines = len(query_lines)

        self.query_seq = ''
        self.match_seq = ''
        self.sbjct_seq = ''
        self.query_start_pos = 9999999
        self.query_end_pos = 0
        self.sbjct_start_pos = 9999999
        self.sbjct_end_pos = 0
        #print 'query_lines=',query_lines
        #print 'match_lines=',match_lines
        #print 'sbjct_lines=',sbjct_lines
        for row_no in xrange(no_align_lines):
            #print ' row_no=', row_no
            query_line = query_lines[row_no]
            match_line = match_lines[row_no]
            sbjct_line = sbjct_lines[row_no]

            [query_start_pos_of_line, query_seq, query_end_pos_of_line] = [item for item in query_line.split(' ') if item != '']
            [sbjct_start_pos_of_line, sbjct_seq, sbjct_end_pos_of_line] = [item for item in sbjct_line.split(' ') if item != '']

            query_start_pos_of_line = int(query_start_pos_of_line)
            query_end_pos_of_line = int(query_end_pos_of_line)
            query_seq_len = len(query_seq)

            sbjct_start_pos_of_line = int(sbjct_start_pos_of_line)
            sbjct_end_pos_of_line = int(sbjct_end_pos_of_line)

            if query_start_pos_of_line < self.query_start_pos:
                self.query_start_pos = query_start_pos_of_line
            if query_end_pos_of_line > self.query_end_pos:
                self.query_end_pos = query_end_pos_of_line
            if sbjct_start_pos_of_line < self.sbjct_start_pos:
                self.sbjct_start_pos = sbjct_start_pos_of_line
            if sbjct_end_pos_of_line > self.sbjct_end_pos:
                self.sbjct_end_pos = sbjct_end_pos_of_line

            self.query_seq += query_seq
            match_column_start = query_line.index(query_seq)
            self.match_seq += match_line[match_column_start:match_column_start + query_seq_len]
            self.sbjct_seq += sbjct_seq

    def map_align_to_query_sbjct(self):
        self.match_pos_to_query_pos = {}
        self.match_pos_to_sbjct_pos = {}
        query_pos = self.query_start_pos
        sbjct_pos = self.sbjct_start_pos
        for pos in xrange(len(self.query_seq)):
            if self.query_seq[pos] != '-':
                self.match_pos_to_query_pos[pos] = query_pos
                query_pos += 1
            if self.sbjct_seq[pos] != '-':
                self.match_pos_to_sbjct_pos[pos] = sbjct_pos
                sbjct_pos += 1

    def find_reliable_match(self):
        self.query_sbjct_match = {}
        len_match_seq = len(self.match_seq)
        for pos in xrange(len_match_seq):
            scan_start = max(0, pos - self.half_scan_size)
            scan_end = min(pos + self.half_scan_size, len_match_seq - 1)

            scan_window = self.match_seq[scan_start : scan_end + 1]

            no_mismatch = scan_window.count(' ') + scan_window.count('+')
            #print query_pos, sbjct_pos, scan_start, scan_end, '['+scan_window+']', no_mismatch
            if no_mismatch <= self.mismatch_allowance:
                #if self.uniprot_acc == 'P20739':
                #    print 'pos=', pos, ', scan_window:', scan_window
                for write_pos in xrange(scan_start, scan_end + 1):
                    aa = self.match_seq[write_pos]
                    if aa != '+' and aa != ' ' and self.match_pos_to_query_pos.has_key(write_pos) and self.match_pos_to_sbjct_pos.has_key(write_pos):
                        query_pos = self.match_pos_to_query_pos[write_pos]
                        sbjct_pos = self.match_pos_to_sbjct_pos[write_pos]
                        #if self.uniprot_acc == 'P20739':
                        #    print 'match:', query_pos, sbjct_pos, self.match_seq[write_pos]
                        self.query_sbjct_match[query_pos] = sbjct_pos

        self.query_poss = self.query_sbjct_match.keys()
        #self.query_poss.sort()
        self.no_match_aa = len(self.query_poss)

    def __init__(self, uid, uniprot_acc, lines):
        self.uid = uid
        self.uniprot_acc = uniprot_acc

        # Finds score and e-value.
        lines = self.find_score_evalue(lines)

        if self.evalue > self.evalue_cutoff:
            return

        # Finds identity and number of identical AAs.
        #for row_no in xrange(len(lines)):
        #    line = lines[row_no]
        #    if len(line) > 13:
        #        if line[:13] == ' Identities =':
        #            self.identity = int(line.split('(')[1].split('%')[0])
        #            self.no_identical_aa = int(line.split('=')[1].split('/')[0])
        #            lines = lines[row_no + 1:]
        #            break

        # Stores query, match, and subject lines from the alignment.
        self.find_query_match_sbjct_lines(lines)

        # Maps align pos to query, sbjct pos.
        self.map_align_to_query_sbjct()

        # Scans to find reliable matches.
        self.find_reliable_match()
    
    def print_query_sbjct_match(self):
        for query_pos in self.query_poss:
            print '\t'.join([str(self.uid), self.uniprot_acc, str(query_pos), str(self.query_sbjct_match[query_pos])])

    def __repr__(self):
        return '\n'.join(['\t'.join([str(self.uid), self.uniprot_acc, str(self.score), str(self.evalue)]), '\t'.join([str(query_pos)+'~'+str(self.query_sbjct_match[query_pos]) for query_pos in self.query_poss])])

    def get_stat(self):
        return '\t'.join([str(self.uid), self.uniprot_acc, str(self.score), str(self.evalue), str(self.no_match_aa)])

class UniProtXrefFix:
    def __init__(self):
        self.query_seq_coverage_cutoff = 0.8
        self.sbjct_seq_coverage_cutoff = 0.8

        self.db = MySQLdb.connect(host='karchin-db01.icm.jhu.edu', user='andywong86', passwd='andy_wong_mysql+86', db='SNVBox_dev')
        self.cursor = self.db.cursor()

    def main(self, write_command):
        #self.find_uid_acc_blast(write_command)
        #self.count_coverage()
        self.load_new_uniprot_xref()

    def load_new_uniprot_xref(self):
        self.cursor.execute('drop table if exists Uniprot_Xref')
        self.db.commit()

        print ' creating Uniprot_Xref...'
        self.cursor.execute('create table Uniprot_Xref (UID int, Pos int, Uniprot varchar(40), UniprotPos int) engine=innodb')
        self.db.commit()

        print ' loading Uniprot_Xref...'
        self.cursor.execute('load data local infile "uniprot_xref_text.txt" into table Uniprot_Xref (UID, Pos, Uniprot, UniprotPos)')
        self.db.commit()

    def count_coverage(self):
        f = open('uid_uniprot_coverage.txt')
        total_count = 0
        blast_count = 0
        cov_pass_count = 0
        for line in f:
            total_count += 1
            [uid, acc, query_cov, sbjct_cov] = line.rstrip().split('\t')
            if query_cov == 'noblast': continue
            blast_count += 1
            query_cov = float(query_cov)
            sbjct_cov = float(sbjct_cov)
            if query_cov >= self.query_seq_coverage_cutoff or\
                sbjct_cov >= self.sbjct_seq_coverage_cutoff:
                cov_pass_count += 1
            #if self.query_seq_coverage_cutoff <= query_cov <= self.query_seq_coverage_cutoff + 0.05 and self.sbjct_seq_coverage_cutoff <= sbjct_cov <= self.sbjct_seq_coverage_cutoff + 0.05:
            #    print uid, acc, query_cov, sbjct_cov
            #    sys.exit()
        f.close()

        print 'total=', total_count, ', blast_count=', blast_count, ', cov_pass=', cov_pass_count, ', %=', float(cov_pass_count)/float(total_count)

    def print_noblast(self, uid, acc):
        print '\t'.join([str(uid), acc, 'noblast', 'noblast'])

    def get_query_seq_len(self, lines):
        # Finds the query sequence length.
        for row_no in xrange(len(lines)):
            line = lines[row_no]
            if len(line) == 0: continue
            if line.count(' letters)') == 1:
                query_seq_len = int(line.replace(',','').split('(')[1].split(' ')[0])
                #print 'line=',line
                #print 'query_seq_len=',query_seq_len
                query_seq_len_row_no = row_no
                break
        lines = lines[query_seq_len_row_no + 1:]
        return query_seq_len, lines

    def get_blast_filename(self, acc):
        acc_head = acc[:2]
        if acc_head == 'NP':
            sub_dir_1 = acc.split('_')[1][:4]
            sub_dir_2 = acc.split('.')[0]
            blast_filename = os.path.join(
                '/projects/snvbox/builds/2013-04/build/blast/uniprot/refseq',
                sub_dir_1,
                sub_dir_2,
                acc + '.blast')
        elif acc_head == 'CC':
            sub_dir_1 = acc[4]
            sub_dir_2 = acc.split('.')[0]
            blast_filename = os.path.join(
                '/projects/snvbox/builds/2013-04/build/blast/uniprot/CCDS',
                sub_dir_1,
                sub_dir_2,
                acc,
                acc + '.blast')
        elif acc_head == 'EN':
            sub_dir_1 = acc[-4:]
            sub_dir_2 = acc
            blast_filename = os.path.join(
                '/projects/snvbox/builds/2013-04/build/blast/uniprot/Ensembl',
                sub_dir_1,
                sub_dir_2,
                acc + '.blast')
        return blast_filename

    def get_uid_to_acc(self):
        f = open('transcript_table.txt')
        uid_to_acc = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aalen] = line.rstrip().split('\t')
            uid = int(uid)
            if refseq_p != 'None':
                uid_to_acc[uid] = refseq_p
            elif ccds != 'None' and uid_to_acc.has_key(uid) == False:
                uid_to_acc[uid] = ccds
            elif ens_p != 'None' and uid_to_acc.has_key(uid) == False:
                uid_to_acc[uid] = ens_p
        f.close()
        return uid_to_acc

    def get_aligns(self, uid, lines, query_seq_len):
        align_start_row_nos = []
        for row_no in xrange(len(lines)):
            line = lines[row_no]
            if len(line) > 4:
                if line[:4] == '>sp|':
                    align_start_row_nos.append(row_no)
                elif line == '  Database: uniprot_sprot.fasta':
                    align_very_end_row_no = row_no - 1
        
        align_end_row_nos = []
        for pos in xrange(1, len(align_start_row_nos)):
            align_end_row_nos.append(align_start_row_nos[pos] - 1)
        align_end_row_nos.append(align_very_end_row_no)

        aligns = []
        for align_no in xrange(len(align_start_row_nos)):
            align_lines = lines[align_start_row_nos[align_no]:align_end_row_nos[align_no] + 1]
            align = BlastMatch(uid, align_lines, query_seq_len)
            aligns.append(align)

        return aligns

    def find_uid_acc_blast(self, write_command):
        if write_command == 'write':
            write_flag = True
        else:
            write_flag = False

        uid_to_acc = self.get_uid_to_acc()
        uids = uid_to_acc.keys()
        uids.sort()

        wf_filename_counter = 0
        last_wf_filename = ''
        while True:
            wf_filename = 'uniprot_xref_text.' + str(wf_filename_counter) + '.txt'
            if os.path.exists(wf_filename):
                last_wf_filename = wf_filename
                wf_filename_counter += 1
            else:
                break

        if last_wf_filename != '':
            print 'last run exists:', last_wf_filename
            print 'reading the last UID processed...'
            f = open(last_wf_filename)
            readline = ''
            for line in f:
                if line != '':
                    readline = line
                continue
            last_uid = int(readline.split('\t')[0])
            print 'last write file is', last_wf_filename
            print 'last processed UID =', last_uid
            print 'resuming from UID', last_uid
        else:
            last_uid = -1

        if write_flag:
            print 'writing to', wf_filename
            wf = open(wf_filename, 'w')

        uid_count = 0
        for uid in uid_to_acc.keys():
            uid_count += 1
            if uid < last_uid: continue
            #if uid_count == 10: break
            acc = uid_to_acc[uid]
            print 'uid=', uid, ', acc=', acc
            blast_filename = self.get_blast_filename(acc)
            
            # If blast file does not exists, go to the next UID.
            if os.path.exists(blast_filename) == False:
                #print ' blast file dose not exist. continuing to the next UID...'
                self.print_noblast(uid, acc)
                continue

            f = open(blast_filename)
            lines = [line[:-1] for line in f.readlines()]
            f.close()
            len_lines = len(lines)

            # If the blast file is empty, go to the next UID.
            if len_lines == 0:
                 self.print_noblast(uid, acc)
                 continue

            query_seq_len, lines = self.get_query_seq_len(lines)

            aligns = self.get_aligns(uid, lines, query_seq_len)
            if len(aligns) >= 1:
                best_align = aligns[0]
                #print 'initial best align:', best_align.no_match_aa
                #print best_align
                for align in aligns[1:]:
                    #print align
                    if align.no_match_aa > best_align.no_match_aa:
                        best_align = align
                        #print '##### best align changed:', best_align.no_match_aa
       
                if write_flag:
                    wf.write(str(best_align) + '\n')
        if write_flag:
            wf.close()

    def check_1st_pass(self, no_identical_aa, query_seq_len, sbjct_seq_len):
        query_seq_coverage = float(no_identical_aa) / float(query_seq_len)
        sbjct_seq_coverage = float(no_identical_aa) / float(sbjct_seq_len)
        if query_seq_coverage < self.query_seq_coverage_cutoff and sbjct_seq_coverage < self.sbjct_seq_coverage:
            return False
        else:
            return True, query_seq_coverage, sbjct_seq_coverage

uf = UniProtXrefFix()

if len(sys.argv) >= 2:
    write_command = sys.argv[1]
else:
    write_command = 'nowrite'

uf.main(write_command)
