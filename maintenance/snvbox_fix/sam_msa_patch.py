import MySQLdb
import os
import sys

bases_to_aa = {'TTT':'F', 'TTC':'F', 'TTA':'L', 'TTG':'L',\
               'CTT':'L', 'CTC':'L', 'CTA':'L', 'CTG':'L',\
               'ATT':'I', 'ATC':'I', 'ATA':'I', 'ATG':'M',\
               'GTT':'V', 'GTC':'V', 'GTA':'V', 'GTG':'V',\
               'TCT':'S', 'TCC':'S', 'TCA':'S', 'TCG':'S',\
               'CCT':'P', 'CCC':'P', 'CCA':'P', 'CCG':'P',\
               'ACT':'T', 'ACC':'T', 'ACA':'T', 'ACG':'T',\
               'GCT':'A', 'GCC':'A', 'GCA':'A', 'GCG':'A',\
               'TAT':'Y', 'TAC':'Y', 'TAA':'*', 'TAG':'*',\
               'CAT':'H', 'CAC':'H', 'CAA':'Q', 'CAG':'Q',\
               'AAT':'N', 'AAC':'N', 'AAA':'K', 'AAG':'K',\
               'GAT':'D', 'GAC':'D', 'GAA':'E', 'GAG':'E',\
               'TGT':'C', 'TGC':'C', 'TGA':'*', 'TGG':'W',\
               'CGT':'R', 'CGC':'R', 'CGA':'R', 'CGG':'R',\
               'AGT':'S', 'AGC':'S', 'AGA':'R', 'AGG':'R',\
               'GGT':'G', 'GGC':'G', 'GGA':'G', 'GGG':'G'}

standard_chroms = ['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9', 'chr10',\
                   'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 'chr16', 'chr17', 'chr18', 'chr19', 'chr20',\
                   'chr21', 'chr22', 'chrX', 'chrY', 'chrM']

class SamMSAPatcher:
    def __init__(self):
        self.db=MySQLdb.connect(host='karchin-db01.icm.jhu.edu',db='SNVBox_dev',user='andywong86',passwd='andy_wong_mysql+86')
        self.cursor=self.db.cursor()

    def main(self):
        #print 'gather UIDs by HUGO'
        #self.gather_uids_by_hugo()
        #print 'match and transfer values from the longest UID'
        #self.match_and_transfer_values_from_the_longest_UID()
        #self.count_uids_in_uids_by_hugo()
        #self.check_uids_from_sam_msa_if_in_uids_by_hugo()
        #print 'checking UID overlap among Transcript, CodonTable, Sam_MSA...'
        #self.check_uids_in_transcript_codontable_sammsa()
        #print 'checking non-standard chromosomes in hg19.2013.gp...'
        #self.check_nonstandard_chromosomes_in_hg192013gp()
        #print 'checking the chromosomes of if in-sammsa, not-in-codontable transcripts...'
        #self.check_chrom_of_insammsa_notincodontable_transcripts()
        #print 'making length-sorted uid list...'
        #self.make_length_sorted_uid_list()
        #print 'splitting CodonTable dump by chromosome...'
        #self.split_codontable_by_chromosome()
        #print 'count accs in CodonTable but not in Transcript table...'
        #self.count_accs_in_codontable_not_in_transcript_table()
        #print 'making CodonTable_UID file...'
        #self.make_codontable_uid()
        #print 'creating and loading CodonTable_UID...'
        #self.create_and_load_codontable_uid()
        #print 'making length-sorted uid list by chromosome...'
        #self.make_length_sorted_uid_list()
        #print 'make chrom pos1 AA UID Pos Sam_MSA_ID...'
        #self.make_chrom_pos1_aa_uid_pos_sammsaid()
        #print 'making uid_pos_sammsaid file...'
        #self.make_uid_pos_sammsaid_file()
        #print 'creating and loading CodonToSamMSA table...'
        #self.create_and_load_codontosammsa_table()
        #print 'making unique SamMSA ID list file from UID Pos to SamMSA ID file...'
        #self.make_unique_sammsaid_from_uid_pos_sammsaid()
        #print 'making trimmed Sam_MSA file...'
        #self.make_trimmed_sammsa_file()
        print 'creating and loading Sam_MSA_unique table...'
        self.create_and_load_sammsaunique_table()

    def create_and_load_sammsaunique_table(self):
        self.cursor.execute('drop table if exists Sam_MSA_unique')
        self.db.commit()

        self.cursor.execute('create table Sam_MSA_unique ' +\
                       '(id int not null auto_increment, ' +\
                        'Entropy double, ' +\
                        'Rel_Entropy double, ' +\
                        'PHC_A double, ' +\
                        'PHC_C double, ' +\
                        'PHC_D double, ' +\
                        'PHC_E double, ' +\
                        'PHC_F double, ' +\
                        'PHC_G double, ' +\
                        'PHC_H double, ' +\
                        'PHC_I double, ' +\
                        'PHC_K double, ' +\
                        'PHC_L double, ' +\
                        'PHC_M double, ' +\
                        'PHC_N double, ' +\
                        'PHC_P double, ' +\
                        'PHC_Q double, ' +\
                        'PHC_R double, ' +\
                        'PHC_S double, ' +\
                        'PHC_T double, ' +\
                        'PHC_V double, ' +\
                        'PHC_W double, ' +\
                        'PHC_Y double, ' +\
                        'PHC_sum double, ' +\
                        'PHC_squaresum double, ' +\
                        'primary key (id)) engine=innodb')
        self.db.commit()

        self.cursor.execute('load data local infile "/home/pipeline/snvbox_fix/trimmed_sam_msa.txt" into table Sam_MSA_unique ('+\
                            'id, Entropy, Rel_Entropy, PHC_A, PHC_C, PHC_D, PHC_E, PHC_F, PHC_G, PHC_H, PHC_I, PHC_K, PHC_L, PHC_M, '+\
                            'PHC_N, PHC_P, PHC_Q, PHC_R, PHC_S, PHC_T, PHC_V, PHC_W, PHC_Y, PHC_sum, PHC_squaresum)')
        self.db.commit()

    def make_trimmed_sammsa_file(self):
        print ' reading target Sam_MSA_IDs...'
        target_sammsaids = {}
        f = open('unique_sammsaid_from_uid_pos_sammsaid.txt')
        for line in f:
            sammsaid = int(line.rstrip())
            target_sammsaids[sammsaid] = 1
        f.close()

        print ' reading original Sam_MSA and writing new Sam_MSA...'
        f = open('sam_msa.txt')
        wf = open('trimmed_sam_msa.txt', 'w')
        count = 0
        for line in f:
            count += 1
            if count % 100000 == 0: print ' ', count
            toks = line.rstrip().split('\t')
            sammsaid = int(toks[0])
            if target_sammsaids.has_key(sammsaid):
                wf.write(str(sammsaid) + '\t' + '\t'.join(toks[3:]) + '\n') # toks[1] = UID, toks[2] = Pos
        f.close()
        wf.close()

    def make_unique_sammsaid_from_uid_pos_sammsaid(self):
        f = open('uid_pos_sammsaid.txt')
        sammsaids_dic = {}
        for line in f:
            [uid, pos, sammsaid] = line.rstrip().split('\t')
            sammsaid = int(sammsaid)
            sammsaids_dic[sammsaid] = 1
        f.close()

        sammsaids = sammsaids_dic.keys()
        sammsaids.sort()

        wf = open('unique_sammsaid_from_uid_pos_sammsaid.txt', 'w')
        for sammsaid in sammsaids:
            wf.write(str(sammsaid) + '\n')
        wf.close()

    def create_and_load_codontosammsa_table(self):
        print ' creating CodonToSamMSA...'
        self.cursor.execute('drop table if exists CodonToSamMSA')
        self.db.commit()
        
        self.cursor.execute('create table CodonToSamMSA (UID int, Pos int unsigned, SamMSAId int) engine=innodb')
        self.db.commit()
        
        print ' loading CodonToSamMSA...'
        self.cursor.execute('load data local infile "/home/pipeline/snvbox_fix/uid_pos_sammsaid.txt" into table CodonToSamMSA (UID, Pos, SamMSAId)')
        self.db.commit()

        print ' creating index...'
        self.cursor.execute('create index CodonToSamMSA_uid_pos on CodonToSamMSA (UID, Pos)')
        self.db.commit()

    def make_uid_pos_sammsaid_file(self):
        f = open('chrom_pos1_aa_uid_pos_sammsaid.txt')
        wf = open('uid_pos_sammsaid.txt', 'w')
        for line in f:
            [chrom, pos1, aa, uid, pos, sammsaid] = line.rstrip().split('\t')
            wf.write('\t'.join([uid, pos, sammsaid]) + '\n')
        wf.close()
        f.close()

    def make_chrom_pos1_aa_uid_pos_sammsaid(self):
        print ' making uid_pos_to_sammsaid...'
        uid_pos_to_sammsaid = {}
        f = open('sammsaid_uid_pos.txt')
        count = 0
        for line in f:
            count += 1
            if count % 10000 == 0: print ' ', count
            #if count % 1000000 == 0:
            #    break
            [sammsaid, uid, pos] = line.rstrip().split('\t')
            uid = int(uid)
            pos = int(pos)
            if uid_pos_to_sammsaid.has_key(uid) == False:
                uid_pos_to_sammsaid[uid] = {}
            uid_pos_to_sammsaid[uid][pos] = sammsaid
        f.close()

        print ' processing UIDs by aalen...'
        chrom_pos1_aa_to_sammsaid = {}
        f = open('uids_sorted_by_aalen.txt')
        wf = open('chrom_pos1_aa_uid_pos_sammsaid.txt', 'w')
        wf_err = open('codontable_uid_pos_not_in_sammsa.txt', 'w')
        count = 0
        for line in f:
            #print '  line:', line.rstrip()
            count += 1
            uid = int(line.split('\t')[0])
            if count % 10000 == 0: print ' count =', count, ', UID =', uid

            if uid_pos_to_sammsaid.has_key(uid) == False:
                #print '  UID', uid, 'not in Sam_MSA, continuing...'
                wf_err.write(str(uid) + '\tall\n')
                continue

            self.cursor.execute('select * from CodonTable_UID where UID=' + str(uid))
            for row in self.cursor.fetchall():
                #print '  row:', row
                (uid, pos, chrom, pos1, dummy, dummy, bases) = row

                if uid_pos_to_sammsaid[uid].has_key(pos) == False:
                    #print '  Pos', pos, 'not in Sam_MSA, continuing...'
                    wf_err.write(str(uid) + '\t' + str(pos) + '\n')
                    continue

                aa = bases_to_aa[bases]
                #print '  aa =', aa

                if chrom_pos1_aa_to_sammsaid.has_key(chrom) == False:
                    chrom_pos1_aa_to_sammsaid[chrom] = {}

                if chrom_pos1_aa_to_sammsaid[chrom].has_key(pos1) == False:
                    chrom_pos1_aa_to_sammsaid[chrom][pos1] = {}

                if chrom_pos1_aa_to_sammsaid[chrom][pos1].has_key(aa) == False:
                    sammsaid = uid_pos_to_sammsaid[uid][pos]
                    chrom_pos1_aa_to_sammsaid[chrom][pos1][aa] = sammsaid
                else:
                    sammsaid = chrom_pos1_aa_to_sammsaid[chrom][pos1][aa]
                #print '  sammsaid =', sammsaid
                
                wf.write('\t'.join([chrom, str(pos1), aa, str(uid), str(pos), str(sammsaid)]) + '\n')
        f.close()
        wf.close()
        wf_err.close()

    def make_length_sorted_uid_list(self):
        class CodonTableRow:
            def __init__(self, line):
                [self.acc, self.pos, self.chrom, self.pos1, dummy, dummy, dummy] = line.rstrip().split('\t')

            def __repr__(self):
                return ', '.join([self.acc, self.pos, self.chrom, self.pos1])

        aalen_to_uid = self.get_aalen_to_uid_dic()

        aalens = aalen_to_uid.keys()
        aalens.sort(reverse=True)

        wf = open('uids_sorted_by_aalen.txt', 'w')
        for aalen in aalens:
            uids = aalen_to_uid[aalen]
            for uid in uids:
                wf.write(uid + '\t' + str(aalen) + '\n')
        wf.close()

        return

        print ' processing CodonTable...'
        for chrom in standard_chroms:
            print ' ', chrom + '...'
            codon_table_rows = []
            f = open('codontable/codontable_' + chrom + '.txt')
            for line in f:
                codon_table_row = CodonTableRow(line)
                #print '   line:', line.rstrip()
                #print '   codon_table_row:', codon_table_row
                if codon_table_row.pos == '1':
                    uid = acc_to_uid[codon_table_row.acc]
                    aalen = acc_to_aalen[codon_table_row.acc]
                    codon_table_row.uid = uid
                    codon_table_row.aalen = aalen
                    codon_table_rows.append(codon_table_row)
            f.close()
            print ' ', len(codon_table_rows), 'CodonTable rows from', chrom

        sys.exit()

        print ' sorting...'
        for i in xrange(len(uids)-1):
            if i % 100 == 0: print ' ', i
            for j in xrange(i+1, len(uids)):
                if uid_to_aalen[uids[i]] < uid_to_aalen[uids[j]]:
                    tmp_uid = uids[i]
                    uids[i] = uids[j]
                    uids[j] = tmp_uid

    def create_and_load_codontable_uid(self):
        print ' creating CodonTable_UID...'
        self.cursor.execute('drop table if exists CodonTable_UID')
        self.db.commit()
        self.cursor.execute('create table CodonTable_UID (UID int, Pos int unsigned, chrom varchar(255), pos1 int unsigned, pos2 int unsigned, pos3 int unsigned, bases char(3)) engine=innodb')
        self.db.commit()

        print ' loading CodonTable_UID...'
        self.cursor.execute('load data local infile "/home/pipeline/snvbox_fix/codontable/codontable_uid.txt" into table CodonTable_UID (UID, Pos, chrom, pos1, pos2, pos3, bases)')
        self.db.commit()

        print ' creating index...'
        self.cursor.execute('create index CodonTable_UID_index_uid_pos on CodonTable_UID (UID, Pos)')
        self.cursor.execute('create index CodonTable_UID_index_chrom_pos on CodonTable_UID (chrom, pos1, pos2, pos3)')
        self.db.commit()

    def make_codontable_uid(self):
        acc_to_uid = self.get_acc_to_uid_dic()

        f = open('codontable/codontable.txt')
        wf = open('codontable/codontable_uid.txt', 'w')
        discarded_accs = {}
        written_uid_poss = {}
        count = 0
        for line in f:
            count += 1
            if count % 100000 == 0: print ' ', count
            [acc, pos, chrom, pos1, pos2, pos3, bases] = line.rstrip().split('\t')
            if acc_to_uid.has_key(acc):
                uid = acc_to_uid[acc]
                uid_pos = uid + '.' + pos
                if written_uid_poss.has_key(uid_pos) == False:
                    wf.write('\t'.join([uid, pos, chrom, pos1, pos2, pos3, bases]) + '\n')
                    written_uid_poss[uid_pos] = 1
            else:
                if discarded_accs.has_key(acc) == False:
                    discarded_accs[acc] = 1
        f.close()
        wf.close()
        
        print len(discarded_accs), 'accs discarded'

        wf = open('accs_discarded_in_making_codontable_uid.txt', 'w')
        for acc in discarded_accs.keys():
            wf.write(acc + '\n')
        wf.close()

    def count_accs_in_codontable_not_in_transcript_table(self):
        f = open('distinct_mrnaaccs_from_codon_table.txt')
        accs_codontable = []
        for line in f:
            accs_codontable.append(line.rstrip())
        f.close()

        f = open('transcript_table.txt')
        accs_transcript_table = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aalen] = line.rstrip().split('\t')
            if ccds != 'None':
                accs_transcript_table[ccds] = 1
            if refseq_t != 'None':
                accs_transcript_table[refseq_t] = 1
            if ens_t != 'None':
                accs_transcript_table[ens_t] = 1
        f.close()

        wf = open('accs_in_codontable_not_in_transcript_table.txt', 'w')
        no_acc_in_codontable_not_in_transcript_table = 0
        for acc in accs_codontable:
            if accs_transcript_table.has_key(acc) == False:
                no_acc_in_codontable_not_in_transcript_table += 1
                wf.write(acc + '\n')
        wf.close()

        print ' ', no_acc_in_codontable_not_in_transcript_table, 'accs in CodonTable but not in Transcript table'

    def split_codontable_by_chromosome(self):
        wfs = {}
        for chrom in standard_chroms:
            wfs[chrom] = open('codontable_' + chrom + '.txt', 'w')

        f = open('codontable.txt')
        count = 0
        for line in f:
            count += 1
            if count % 100000 == 0: print format(count, ' ,d')
            toks = line.split('\t')
            chrom = toks[2]
            wfs[chrom].write(line)
        f.close()

        for chrom in standard_chroms:
            wfs[chrom].close()

    def check_chrom_of_insammsa_notincodontable_transcripts(self):
        f = open('hg19.2013.gp')
        acc_to_chrom = {}
        for line in f:
            toks = line.split('\t')
            [acc, chrom] = toks[:2]
            acc_to_chrom[acc] = chrom
        f.close()

        no_in_transcript_not_in_hg192013gp = 0
        no_standard_chroms = 0
        no_nonstandard_chroms = 0
        wf_intranscript_not_in_hg192013gp = open('transcripts_in_sammsa_not_in_codontable.in_transcript_not_in_hg192013gp.txt', 'w')
        wf_standard_chroms = open('transcripts_in_sammsa_not_in_codontable.stardand_chroms.txt', 'w')
        wf_nonstandard_chroms = open('transcripts_in_sammsa_not_in_codontable.nonstardand_chroms.txt', 'w')
        f = open('transcripts_in_sammsa_not_in_codontable.txt')
        for line in f:
            toks = line.rstrip().split('\t')
            uid = toks[0]
            accs = toks[1:]
            for acc in accs:
                if acc.count('.') > 0:
                    acc = acc.split('.')[0]
                if acc_to_chrom.has_key(acc) == False:
                    no_in_transcript_not_in_hg192013gp += 1
                    wf_intranscript_not_in_hg192013gp.write(uid + '\t' + 'NA' + '\t' + '\t'.join(accs) + '\n')
                    break
                else:
                    chrom = acc_to_chrom[acc]
                    if chrom in standard_chroms:
                        no_standard_chroms += 1
                        wf_standard_chroms.write(uid + '\t' + chrom + '\t' + '\t'.join(accs) + '\n')
                        print uid, chrom, accs
                        break
                    else:
                        no_nonstandard_chroms += 1
                        wf_nonstandard_chroms.write(uid + '\t' + chrom + '\t' + '\t'.join(accs) + '\n')
                        break
        wf_intranscript_not_in_hg192013gp.close()
        wf_standard_chroms.close()
        wf_nonstandard_chroms.close()
        f.close()
        print ' ', no_in_transcript_not_in_hg192013gp, 'transcripts not in hg19.2013.gp'
        print ' ', no_standard_chroms, 'transcripts with standard chromosomes'
        print ' ', no_nonstandard_chroms, 'transcripts with non-standard chromosomes'

    def check_nonstandard_chromosomes_in_hg192013gp(self):
        f = open('hg19.2013.gp')
        no_occur_nonstandard_chroms = 0
        observed_nonstandard_chroms = {}
        for line in f:
            chrom = line.split('\t')[1]
            if not chrom in standard_chroms:
                no_occur_nonstandard_chroms += 1
                if observed_nonstandard_chroms.has_key(chrom) == False:
                    observed_nonstandard_chroms[chrom] = 1
        f.close()

        print no_occur_nonstandard_chroms, 'occurrences of nonstandard chromosomes'
        print 'observed nonstandard choromosomes are', ', '.join(observed_nonstandard_chroms.keys())

    def get_acc_to_uid_dic(self):
        f = open('transcript_table.txt')
        acc_to_uid = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len] = line.rstrip().split('\t')
            if refseq_t != 'None':
                acc_to_uid[refseq_t] = uid
            if ccds != 'None':
                acc_to_uid[ccds] = uid
            if ens_t != 'None':
                acc_to_uid[ens_t] = uid
        f.close()
        return acc_to_uid

    def get_acc_to_aalen_dic(self):
        f = open('transcript_table.txt')
        acc_to_aalen = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len] = line.rstrip().split('\t')
            if refseq_t != 'None':
                acc_to_aalen[refseq_t] = aa_len
            if ccds != 'None':
                acc_to_aalen[ccds] = aa_len
            if ens_t != 'None':
                acc_to_aalen[ens_t] = aa_len
        f.close()
        return acc_to_aalen

    def get_uid_to_aalen_dic(self):
        f = open('transcript_table.txt')
        uid_to_aalen = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aalen] = line.rstrip().split('\t')
            uid_to_aalen[uid] = aalen
        f.close()
        return uid_to_aalen

    def get_aalen_to_uid_dic(self):
        f = open('transcript_table.txt')
        aalen_to_uid = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aalen] = line.rstrip().split('\t')
            if aalen == 'None':
                aalen = 0
            else:
                aalen = int(aalen)
            if aalen_to_uid.has_key(aalen) == False:
                aalen_to_uid[aalen] = []
            aalen_to_uid[aalen].append(uid)
        f.close()
        return aalen_to_uid

    def check_uids_in_transcript_codontable_sammsa(self):
        f = open('transcript_table.txt')
        acc_to_uid = {}
        uid_to_acc = {}
        uids_in_transcript = {}
        for line in f:
            [uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len] = line.rstrip().split('\t')
            uid_to_acc[uid] = []
            if refseq_t != 'None':
                acc_to_uid[refseq_t] = uid
                uid_to_acc[uid].append(refseq_t)
            if ccds != 'None':
                acc_to_uid[ccds] = uid
                uid_to_acc[uid].append(ccds)
            if ens_t != 'None':
                acc_to_uid[ens_t] = uid
                uid_to_acc[uid].append(ens_t)
            uids_in_transcript[uid] = 1
        f.close()
        #print len(uids_in_transcript_table), 'UIDs in Transcript table'

        # CodonTable
        f = open('distinct_mrnaaccs_from_codon_table.txt')
        uids_in_codontable = {}
        for line in f:
            acc = line.rstrip()
            if acc_to_uid.has_key(acc):
                uids_in_codontable[acc_to_uid[acc]] = 1
        f.close()
        #print len(uids_in_codon_table), 'UIDs in CodonTable table'

        # Sam_MSA
        f = open('distinct_uid_from_sam_msa.txt')
        uids_in_sammsa = {}
        for line in f:
            uid = line.rstrip()
            uids_in_sammsa[uid] = 1
        f.close()

        # Compares UIDs from CodonTable and Sam_MSA.
        no_uid_in_all = 0
        no_uid_in_transcript_codontable = 0
        no_uid_in_transcript_sammsa = 0
        no_uid_in_transcript = 0
        for uid in uids_in_transcript.keys():
            in_codontable = uids_in_codontable.has_key(uid)
            in_sammsa = uids_in_sammsa.has_key(uid)
            if in_codontable and in_sammsa:
                no_uid_in_all += 1
            elif in_codontable and not in_sammsa:
                no_uid_in_transcript_codontable += 1
            elif not in_codontable and in_sammsa:
                no_uid_in_transcript_sammsa += 1
                if uid_to_acc[uid][0][:2] != 'EN':
                    print uid, uid_to_acc[uid]
            else:
                no_uid_in_transcript += 1
        print len(uids_in_transcript), 'UIDs in Transcript'
        print ' ', no_uid_in_all, 'UIDs in Transcript, CodonTable, and Sam_MSA'
        print ' ', no_uid_in_transcript_codontable, 'UIDs in Transcript, CodonTable'
        print ' ', no_uid_in_transcript_sammsa, 'UIDs in Transcript, Sam_MSA'
        print ' ', no_uid_in_transcript, 'UIDs in Transcript only'

        no_uid_in_all = 0
        no_uid_in_codontable_transcript = 0
        no_uid_in_codontable_sammsa = 0
        no_uid_in_codontable = 0
        for uid in uids_in_codontable.keys():
            in_transcript = uids_in_transcript.has_key(uid)
            in_sammsa = uids_in_sammsa.has_key(uid)
            if in_transcript and in_sammsa:
                no_uid_in_all += 1
            elif in_transcript and not in_sammsa:
                no_uid_in_codontable_transcript += 1
            elif not in_transcript and in_sammsa:
                no_uid_in_codontable_sammsa += 1
            else:
                no_uid_in_codontable += 1
        print len(uids_in_codontable), 'UIDs in CodonTable'
        print ' ', no_uid_in_all, 'UIDs in CodonTable, Transcript, Sam_MSA'
        print ' ', no_uid_in_codontable_transcript, 'UIDs in CodonTable, Transcript'
        print ' ', no_uid_in_codontable_sammsa, 'UIDs in CodonTable, Sam_MSA'
        print ' ', no_uid_in_codontable, 'UIDs in CodonTable only'

        no_uid_in_all = 0
        no_uid_in_sammsa_transcript = 0
        no_uid_in_sammsa_codontable = 0
        no_uid_in_sammsa = 0
        wf = open('transcripts_in_sammsa_not_in_codontable.txt', 'w')
        for uid in uids_in_sammsa.keys():
            in_transcript = uids_in_transcript.has_key(uid)
            in_codontable = uids_in_codontable.has_key(uid)
            if in_transcript and in_codontable:
                no_uid_in_all += 1
            elif in_transcript and not in_codontable:
                no_uid_in_sammsa_transcript += 1
                wf.write(uid + '\t' + '\t'.join(uid_to_acc[uid]) + '\n')
            elif not in_transcript and in_codontable:
                no_uid_in_sammsa_codontable += 1
            else:
                no_uid_in_sammsa += 1
        wf.close()
        print len(uids_in_sammsa), 'UIDs in Sam_MSA'
        print ' ', no_uid_in_all, 'UIDs in Sam_MSA, Transcript, CodonTable'
        print ' ', no_uid_in_sammsa_transcript, 'UIDs in Sam_MSA, Transcript'
        print ' ', no_uid_in_sammsa_codontable, 'UIDs in Sam_MSA, CodonTable'
        print ' ', no_uid_in_sammsa, 'UIDs in Sam_MSA only'

    def check_uids_from_sam_msa_if_in_uids_by_hugo(self):
        f = open('uids_by_hugo.txt')
        uids_from_uids_by_hugo = {}
        for line in f:
            toks = line.rstrip().split('\t')
            line_uids = toks[4].split(',')
            for uid in line_uids:
                if uids_from_uids_by_hugo.has_key(uid):
                    print 'duplicate uid:', uid, ':', line.rstrip()
                    sys.exit()
                uids_from_uids_by_hugo[uid] = 1
        f.close()
        
        f = open('distinct_uid_from_sam_msa.txt')
        uids_from_sam_msa = {}
        for line in f:
            uid = line.rstrip()
            uids_from_sam_msa[uid] = 1
        f.close()

        no_uid_in_uids_by_hugo_count = 0
        for uid in uids_from_sam_msa.keys():
            if uids_from_uids_by_hugo.has_key(uid) == False:
                print uid, 'not in uids_by_hugo'
                no_uid_in_uids_by_hugo_count += 1
        print no_uid_in_uids_by_hugo_count, 'UIDs from Sam_MSA not in uids_by_hugo'

    def count_uids_in_uids_by_hugo(self):
        f = open('uids_by_hugo.txt')
        uids = {}
        for line in f:
            toks = line.rstrip().split('\t')
            line_uids = toks[4].split(',')
            for uid in line_uids:
                if uids.has_key(uid):
                    print 'duplicate uid:', uid, ':', line.rstrip()
                    sys.exit()
                uids[uid] = 1
        f.close()
        print len(uids), 'UIDs in uids_by_hugo.txt'
    
    def gather_uids_by_hugo(self):
        self.cursor.execute('select t.*, g.* from Transcript as t, GeneSymbols as g where t.UID=g.UID')
        results = self.cursor.fetchall()
        uids_by_hugo = {}
        for row in results:
            (uid, ccds, refseq_t, refseq_p, ens_t, ens_p, aa_len, dummy, hugo, ncbi_gene_id) = row
            if uids_by_hugo.has_key(hugo) == False:
                uids_by_hugo[hugo] = {}
            if refseq_p != None:
                acc_t = refseq_t
                acc_p = refseq_p
            elif ccds != None:
                acc_t = ccds
                acc_p = ccds
            elif ens_p != None:
                acc_t = ens_t
                acc_p = ens_p
            else:
                print 'error', row
                sys.exit()

            uids_by_hugo[hugo][uid] = {'acc_t':acc_t, 'acc_p':acc_p, 'len':aa_len}
        
        hugos = uids_by_hugo.keys()
        hugos.sort()
        
        wf = open('uids_by_hugo.txt', 'w')
        for hugo in hugos:
            aa_lens = []
            for uid in uids_by_hugo[hugo].keys():
                aa_lens.append(uids_by_hugo[hugo][uid]['len'])
            max_aa_len = max(aa_lens)

            longest_uid = uids_by_hugo[hugo].keys()[0]
            longest_acc = ''
            for uid in uids_by_hugo[hugo].keys():
                aa_len = uids_by_hugo[hugo][uid]['len']
                if aa_len == max_aa_len:
                    longest_uid = uid
                    longest_acc_t = uids_by_hugo[hugo][uid]['acc_t']
                    longest_acc_p = uids_by_hugo[hugo][uid]['acc_p']
                    break

            wf.write(str(hugo) + '\t' + str(longest_uid) + '\t' + longest_acc_t + '\t' + longest_acc_p + '\t' + ','.join([str(uid) for uid in uids_by_hugo[hugo].keys()]) + '\t' + ','.join([uids_by_hugo[hugo][uid]['acc_t'] for uid in uids_by_hugo[hugo].keys()]) + '\t' + ','.join([uids_by_hugo[hugo][uid]['acc_p'] for uid in uids_by_hugo[hugo].keys()]) + '\n')
        wf.close()
    
    def get_exon_ranges(self, uid, acc_p):
        self.cursor.execute('select aaLen from Transcript where UID=' + uid)
        aa_len = self.cursor.fetchone()[0]

        self.cursor.execute('select Exon, Str, aaStart, aaEnd from Transcript_Exon where UID=' + uid)
        results = self.cursor.fetchall()
        aa_start_end_by_exon = {}
        exons = []
        for row in results:
            (exon, strand, aa_start, aa_end) = row
            exons.append(exon)
            aa_start_end_by_exon[exon] = [aa_start, aa_end]

        #self.cursor.execute('select codon, bases from CodonTable where mrnaAcc="' + acc_t + '"')
        #aas = ' '
        #for row in self.cursor.fetchall():
        #    (pos, bases) = row
        #    aa = bases_to_aa[bases]
        #    aas += aa
        fa_dir_base = '/projects/snvbox/builds/2013-04/build/t2k-hmms/'
        if acc_p[:2] == 'NP':
            sub_dir = acc_p[3:7]
            fa_filename = os.path.join(fa_dir_base, 'refseq', sub_dir, acc_p.split('.')[0], acc_p + '.fa')
        elif acc_p[:2] == 'CC':
            sub_dir = acc_p[4]
            fa_filename = os.path.join(fa_dir_base, 'CCDS', sub_dir, acc_p.split('.')[0], acc_p + '.fa')
        elif acc_p[:2] == 'EN':
            sub_dir = acc_p[-4:]
            fa_filename = os.path.join(fa_dir_base, 'Ensembl', sub_dir, acc_p.split('.')[0], acc_p + '.fa')
        else:
            print 'error. acc_p=', acc_p
            sys.exit()

        f = open(fa_filename)
        f.readline()
        aas = ''
        for line in f:
            aas += line.strip()
        f.close()

        aas_by_exon = {}
        for exon in aa_start_end_by_exon.keys():
            [aa_start, aa_end] = aa_start_end_by_exon[exon]
            if aa_start == 0 and aa_end == 0:
                aas_by_exon[exon ] = ''
            else:
                aas_by_exon[exon] = aas[aa_start - 1:aa_end]
        return exons, aa_start_end_by_exon, aas_by_exon, strand, aa_len

    def get_sam_msa_values(self, uid):
        self.cursor.execute('select * from Sam_MSA where UID=' + uid)
        sam_msa_by_aa_pos = {}
        for row in self.cursor.fetchall():
            (dummy, aa_pos, entropy, rel_entropy, phc_a, phc_c, phc_d, phc_e, phc_f, phc_g, phc_h, phc_i, phc_k, phc_l, phc_m, phc_n, phc_p, phc_q, phc_r, phc_s, phc_t, phc_v, phc_w, phc_y, phc_sum, phc_squaresum) = row
            sam_msa_by_aa_pos[aa_pos] = [entropy, rel_entropy, phc_a, phc_c, phc_d, phc_e, phc_f, phc_g, phc_h, phc_i, phc_k, phc_l, phc_m, phc_n, phc_p, phc_q, phc_r, phc_s, phc_t, phc_v, phc_w, phc_y, phc_sum, phc_squaresum]
        return sam_msa_by_aa_pos

    def match_and_transfer_values_from_the_longest_UID(self):
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

        f = open('uids_by_hugo.txt')
        uids_by_hugo = {}
        for line in f:
            toks = line.rstrip().split('\t')
            [hugo, longest_uid, longest_uid_acc_t, longest_uid_acc_p, uids, acc_ts, acc_ps] = toks
            if hugo != 'A1CF': continue
            print 'hugo =', hugo, ', longest UID =', longest_uid, ', longest acc =', longest_uid_acc_t, longest_uid_acc_p
            uids = uids.split(',')
            acc_ts = acc_ts.split(',')
            acc_ps = acc_ps.split(',')
            uid_to_acc_t = {}
            uid_to_acc_p = {}
            for i in xrange(len(uids)):
                uid_to_acc_t[uids[i]] = acc_ts[i]
                uid_to_acc_p[uids[i]] = acc_ps[i]

            print ' collecting aas by exon...'
            aa_range_by_uid_exon = {}
            aas_by_uid_exon = {}
            aa_lens_by_uid = {}
            for uid in uids:
                acc_p = uid_to_acc_p[uid]
                exons, aa_start_end_by_exon, aas_by_exon, strand, aa_len = self.get_exon_ranges(uid, acc_p)
                aa_range_by_uid_exon[uid] = aa_start_end_by_exon
                aas_by_uid_exon[uid] = aas_by_exon
                aa_lens_by_uid[uid] = aa_len
                print '  UID=', uid
                #print '   start_end=', aa_start_end_by_exon
                print '   aas by exon:'
                for exon in aas_by_exon.keys():
                    print '  ', exon, ':', aas_by_exon[exon]

            print ' collecting sam_msa values by uid and aa pos...'
            sam_msa_by_uid_aa_pos = {}
            for uid in uids:
                sam_msa_by_aa_pos = self.get_sam_msa_values(uid)
                sam_msa_by_uid_aa_pos[uid] = sam_msa_by_aa_pos

            print ' comparing...'
            longest_uid_exons = aas_by_uid_exon[longest_uid].keys()
            if strand == '+':
                longest_uid_exons.sort()
            else:
                longest_uid_exons.sort(reverse=True)

            for uid in uids:
                if uid == longest_uid: continue
                print '  UID=', uid, uid_to_acc_t[uid]
                short_uid = uid
                short_uid_exons = aas_by_uid_exon[short_uid].keys()
                if strand == '+':
                    short_uid_exons.sort()
                else:
                    short_uid_exons.sort(reverse=True)

                short_uid_aas_by_exon = aas_by_uid_exon[short_uid]
                longest_uid_aas_by_exon = aas_by_uid_exon[longest_uid]

                short_uid_aa_len = aa_lens_by_uid[short_uid]
                longest_uid_aa_len = aa_lens_by_uid[longest_uid]

                short_to_longest_exon_match = {}
                longest_uid_exon_pos_to_start_compare = 0
                for short_uid_exon in short_uid_exons:
                    short_uid_exon_aas = aas_by_uid_exon[short_uid][short_uid_exon]
                    if short_uid_exon_aas == '': continue
                    match_found = False
                    for longest_uid_exon in longest_uid_exons[longest_uid_exon_pos_to_start_compare:]:
                        if short_uid_aas_by_exon[short_uid_exon] == longest_uid_aas_by_exon[longest_uid_exon]:
                            short_to_longest_exon_match[short_uid_exon] = longest_uid_exon
                            longest_uid_exon_pos_to_start_compare = longest_uid_exons.index(longest_uid_exon) + 1
                            print '  ', short_uid_exon, ': matched with', longest_uid_exon, ', longest_uid_exon_pos_to_start_compare =', longest_uid_exon_pos_to_start_compare
                            match_found = True

                            # Writes the same SAm MSA values from the longest transcript, in place of the same amino acids of the shorter transcripts.
                            [short_uid_aa_start, short_uid_aa_end] = aa_range_by_uid_exon[short_uid][short_uid_exon]
                            [longest_uid_aa_start, longest_uid_aa_end] = aa_range_by_uid_exon[longest_uid][longest_uid_exon]
                            if (short_uid_aa_end - short_uid_aa_start + 1) != (longest_uid_aa_end - longest_uid_aa_start + 1):
                                print 'uid aa start end mismatch', short_uid, longest_uid, short_uid_aa_start, short_uid_aa_end, longest_uid_aa_start, longest_uid_aa_end
                                sys.exit()

                            short_uid_sam_msa_by_aa_pos = sam_msa_by_uid_aa_pos[short_uid]
                            longest_uid_sam_msa_by_aa_pos = sam_msa_by_uid_aa_pos[longest_uid]
                            for relative_aa_pos in xrange(short_uid_aa_end - short_uid_aa_start + 1):
                                short_uid_aa_pos = short_uid_aa_start + relative_aa_pos
                                longest_uid_aa_pos = longest_uid_aa_start + relative_aa_pos
                                if short_uid_aa_pos >= short_uid_aa_len:
                                    break
                                #print short_uid, short_uid_aa_pos, longest_uid_sam_msa_by_aa_pos[longest_uid_aa_pos]
                            break
                    if match_found == False:
                        print '  ', short_uid_exon, ': no match found. longest_uid_exon_pos_to_start_compare =', longest_uid_exon_pos_to_start_compare

            sys.exit()

smp = SamMSAPatcher()
smp.main()
