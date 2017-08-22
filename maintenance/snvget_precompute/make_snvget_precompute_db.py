import os
import sys
import MySQLdb

db = MySQLdb.connect(host='karchin-db01.icm.jhu.edu', user='andywong86', passwd='andy_wong_mysql+86', db='snvget_precompute_dev')
cursor = db.cursor()

def drop_and_create_tables():
    chroms = ['chr'+str(n) for n in range(1, 23)]
    chroms.extend(['chrX', 'chrY'])

    for chrom in chroms:
        print chrom
        table = chrom
        cursor.execute('drop table if exists ' + table)
        db.commit()

        cursor.execute('create table ' + table + ' (' + 
            'chrom varchar(5), '+
            'pos int, '+
            'ref char(1), '+
            'alt char(1), '+
            'accession varchar(20), '+
            'aachange varchar(10), '+
            'AABLOSUM float, '+
            'AACharge float, '+
            'AACOSMIC float, '+
            'AACOSMICvsHapMap float, '+
            'AACOSMICvsSWISSPROT float, '+
            'AAEx float, '+
            'AAGrantham float, '+
            'AAHapMap float, '+
            'AAHGMD2003 float, '+
            'AAHydrophobicity float, '+
            'AAMJ float, '+
            'AAPAM250 float, '+
            'AAPolarity float, '+
            'AATransition float, '+
            'AAVB float, '+
            'AAVolume float, '+
            'MGAEntropy float, '+
            'MGAPHC float, '+
            'MGARelEntropy float, '+
            'HMMEntropy float, '+
            'HMMPHC float, '+
            'HMMRelEntropy float, '+
            'ExonConservation float, '+
            'ExonHapMapSnpDensity float, '+
            'ExonSnpDensity float, '+
            'PredBFactorF float, '+
            'PredBFactorM float, '+
            'PredBFactorS float, '+
            'PredRSAB float, '+
            'PredRSAE float, '+
            'PredRSAI float, '+
            'PredSSC float, '+
            'PredSSE float, '+
            'PredSSH float, '+
            'PredStabilityH float, '+
            'PredStabilityL float, '+
            'PredStabilityM float, '+
            'RegCompC float, '+
            'RegCompDE float, '+
            'RegCompEntropy float, '+
            'RegCompG float, '+
            'RegCompH float, '+
            'RegCompILVM float, '+
            'RegCompKR float, '+
            'RegCompNormEntropy float, '+
            'RegCompP float, '+
            'RegCompQ float, '+
            'RegCompWYF float, '+
            'AATripletFirstProbMut float, '+
            'AATripletFirstProbWild float, '+
            'AATripletSecondProbMut float, '+
            'AATripletSecondProbWild float, '+
            'AATripletThirdProbMut float, '+
            'AATripletThirdProbWild float, '+
            'AATripletFirstDiffProb float, '+
            'AATripletSecondDiffProb float, '+
            'AATripletThirdDiffProb float, '+
            'UniprotACTSITE float, '+
            'UniprotBINDING float, '+
            'UniprotCABIND float, '+
            'UniprotCARBOHYD float, '+
            'UniprotCOMPBIAS float, '+
            'UniprotDISULFID float, '+
            'UniprotDNABIND float, '+
            'UniprotDOM_Chrom float, '+
            'UniprotDOM_LOC float, '+
            'UniprotDOM_MMBRBD float, '+
            'UniprotDOM_PostModEnz float, '+
            'UniprotDOM_PostModRec float, '+
            'UniprotDOM_PPI float, '+
            'UniprotDOM_RNABD float, '+
            'UniprotDOM_TF float, '+
            'UniprotLIPID float, '+
            'UniprotMETAL float, '+
            'UniprotMODRES float, '+
            'UniprotMOTIF float, '+
            'UniprotNPBIND float, '+
            'UniprotPROPEP float, '+
            'UniprotREGIONS float, '+
            'UniprotREP float, '+
            'UniprotSECYS float, '+
            'UniprotSIGNAL float, '+
            'UniprotSITE float, '+
            'UniprotTRANSMEM float, '+
            'UniprotZNFINGER float) engine=innodb')
        db.commit()

def dump_tables():
    dump_filenames = [filename for filename in os.listdir('.') if filename.find('.snvgetprecompute.txt') >= 0]
    for dump_filename in dump_filenames:
        #dump_filename = 'chr1_test.snvgetprecompute.txt'
        print dump_filename
        chrom = dump_filename.split('_')[0]
        table = chrom

        cursor.execute('load data local infile "' + dump_filename + '" into table ' + table)
        db.commit()

drop_and_create_tables()
dump_tables()
