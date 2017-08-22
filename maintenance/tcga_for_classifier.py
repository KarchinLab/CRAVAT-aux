#Using Hannah Carter's parseMaf (hn01:/projects/TCGA-mutation-analysis/2013-03-12/src/parseMaf

import sys, os

class MafReader(object):
   def __init__(self, filename):
      self.fh = file(filename, 'r')
      header = self.fh.readline()
      while header[:11] != 'Hugo_Symbol':
          header = self.fh.readline()

   def __iter__(self):
      return self

   def next(self):
      while True:
         line = self.fh.readline().strip()
         if line == '':
            self.fh.close()
            raise StopIteration
         line = line[:-1]
         return MafEntry(line.split("\t"))

class MafEntry(object):
   index = 1

   def __init__(self, row):
      self.index = MafEntry.index
      MafEntry.index += 1
      self.Hugo_Symbol = row[0]
      try:
          self.Entrez_Gene_Id = row[1]
      except Exception, e:
          print row
          sys.exit()
      self.Center = row[2]
      self.NCBI_Build = row[3]
      self.Chromosome = row[4]
      if not self.Chromosome.startswith("chr"):
         self.Chromosome = "chr" + self.Chromosome
      self.Start_position = str(int(row[5])-1)
      self.End_position = row[6]
      self.Strand = row[7]
      self.Variant_Classification = row[8]
      self.Variant_Type = row[9]
      self.Reference_Allele = row[10]
      self.Tumor_Seq_Allele1 = row[11]
      self.Tumor_Seq_Allele2 = row[12]
      self.dbSNP_RS = row[13]
      self.dbSNP_Val_Status = row[14]
      self.Tumor_Sample_Barcode = row[15]
      self.Matched_Norm_Sample_Barcode = row[16]
      self.Match_Norm_Seq_Allele1 = row[17]
      self.Match_Norm_Seq_Allele2 = row[18]
      self.Tumor_Validation_Allele1 = row[19]
      self.Tumor_Validation_Allele2 = row[20]
      self.Match_Norm_Validation_Allele1 = row[21]
      self.Match_Norm_Validation_Allele2 = row[22]
      self.Verification_Status = row[23]
      self.Validation_Status = row[24]
      self.Mutation_Status = row[25]
      self.Sequencing_Phase = row[26]
      self.Sequence_Source = row[27]
      self.Validation_Method = row[28]
      self.Score = row[29]
      self.BAM_File = row[30]
      self.Sequencer = row[31]
      self.Alt_Allele = ""
      self.__getAltAllele()

   def __getAltAllele(self):
      altAlleles = [self.Tumor_Seq_Allele1, self.Tumor_Seq_Allele2]
      if self.Reference_Allele in altAlleles:
         altAlleles.remove(self.Reference_Allele)
      altAlleles = list(set(altAlleles))
      if len(altAlleles) == 1:
         self.Alt_Allele = altAlleles[0]
      else:
         sys.stderr.write("Compound heterozygote detected\n")
         sys.exit(0)

   def __repr__(self):
      # THIS LINE FOR CRAVAT
      #return "\t".join([str(self.index), self.Chromosome, self.Start_position, self.End_position, self.Strand, self.Reference_Allele, self.Alt_Allele, self.Tumor_Sample_Barcode])
      # THIS LINE FOR REGULAR CHASM
      return "\t".join([self.Tumor_Sample_Barcode, \
                        self.Chromosome, \
                        self.Start_position, \
                        self.End_position, \
                        self.Strand, \
                        self.Reference_Allele, \
                        self.Alt_Allele])

def main():
   for entry in MafReader(sys.argv[1]):
      if entry.Variant_Type == "SNP":
         print entry

tcga_dir = sys.argv[1]
tissues = os.listdir(tcga_dir)
for tissue in tissues:
    print tissue
    tissue_dir = os.path.join(tcga_dir, tissue)
    total_maf_filename = os.path.join(tissue_dir, tissue + '.total.missense.maf')
    if os.path.exists(total_maf_filename):
        os.remove(total_maf_filename)
    tcga_filenames = os.listdir(tissue_dir)
#    print '  ', tcga_filenames
    wf = open(total_maf_filename, 'w')
    unique_entries = {}
    for tcga_filename in tcga_filenames:
        print '  ',tcga_filename
        for entry in MafReader(os.path.join(tissue_dir, tcga_filename)):
            if entry.Variant_Type == 'SNP' and entry.Variant_Classification == 'Missense_Mutation':
                entry_str = str(entry)
                if unique_entries.has_key(entry_str) == False:
                    unique_entries[entry_str] = 1
                    wf.write(str(entry) + '\n')
    wf.close()
    print '  ', len(unique_entries), 'entries'