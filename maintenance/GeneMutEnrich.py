#!/usr/bin/env python
'''
Tests for gene mutation enrichment in TCGA data for single genes
Algorithm developed by Dr. Donald Geman

REQUIREMENTS:
- tcga.py to be in the same directory
- Parsed TCGA data to be in the inputdir directory (see parameters below)

Tested using pypy (python 2.7) on Fedora 16
'''
__author__ = "Wing Chung Wong"
__copyright__ = "Copyright 2012, Johns Hopkins University"
__credits__ = ["Donald Geman", "Rachel Karchin", "Rick Kim", "Michael Ryan", "Wing Chung Wong"]
__license__ = "JHU"
__version__ = "1.0"
__maintainer__ = "Rick Kim"
__email__ = "rkim@insilico.us.com"
__status__ = "Under maintenance"

import sys, os, random, copy, math
import argparse,string
from tcga import TCGA

######################################################################################
#  Hard coded parameters that are not meant to be specifiable over the command line
#
######################################################################################

inputdir = "database" # Location of the TCGA data files
genelenll = 10        # Minimum length of gene (approx)
genelenul = 3000      # Maximum length of gene (approx)
binpercentwidth = 10  # +/- percentage of each bin size
binminwidth = 40      # Minimum width of each bin


######################################################################################
#  Mathematical Support Classes and Functions
#
######################################################################################

class PoissonTable:
    def __init__ (self, l, max_x):
        '''
        Creates a Poission dictionary
        '''
        self.max_x = max_x
        self.poisson = {}
        self.cumulative = {}
        self.e_pow_minus_l = math.exp(-l)
        self.poisson[0] = self.e_pow_minus_l
        self.cumulative[0] = self.poisson[0]
        for x in xrange(1, max_x):
            self.poisson[x] = self.poisson[x-1]*l / x
            self.cumulative[x] = self.cumulative[x-1] + self.poisson[x]
            
    def get_max_x (self):
        '''
        Returns max_x
        '''
        return self.max_x
    
    def get_pvalue (self, x):
        '''
        Returns the pvalue for x. If x > max_x, it returns 0 because not possible.
        ''' 
        if self.poisson.has_key(x):
            return self.poisson[x]
        else:
            return 0 # Just means not possible

    def get_cumulative (self, x):
        '''
        Returns the cumulative pvalue for x. If x > max_x, it returns 1, and if x < 0 returns 0.
        '''
        x = int(x)
        if self.cumulative.has_key(x):
            return self.cumulative[x]
        else:
            if x < 0:
                return 0
            else:
                return 1

def createMatrices(AllGenes, TestGenes, TumorSamples, Binnum):
    '''
    NOTE: Need to ensure that TumorSamples is iterated through in the same
          order as in createEtaMinusMatrix
    '''
    etaplusmatrix = []
    etaminusmatrix = []
    nonzerocounts = []
    M = 0
    N = len(AllGenes)
    # Speed up calculation by caching values for each sample
    cachedictplus = {}
    cachedictminus = {}

    # Ensure that the order of iteration does not change
    TumorSampleOrder = TumorSamples.keys()
    for thisGene in AllGenes:
        plusrow = []
        minusrow = []
        NonZeroCount = 0
        thisCountSum = 0
        for samplename in TumorSampleOrder:
            SampleMut = TumorSamples[samplename].MutCountsByBin(Binnum)
            thisCount = SampleMut.get(thisGene, 0)
            NonZeroCount = NonZeroCount + (1 if thisCount > 0 else 0)
            EtaPlusCount = 0
            EtaMinusCount = 0
            if (samplename, thisCount) in cachedictplus:
                EtaPlusCount = cachedictplus[(samplename, thisCount)]
                EtaMinusCount = cachedictminus[(samplename, thisCount)]
            else:
                EtaPlusCount = 0
                EtaMinuscount = 0
                for thatGene in TestGenes:
                    thatCount = SampleMut.get(thatGene,0)
                    if thisCount > thatCount:
                        EtaPlusCount = EtaPlusCount + 1
                    if thisCount < thatCount:
                        EtaMinusCount = EtaMinusCount + 1
                cachedictplus[(samplename, thisCount)] = EtaPlusCount
                cachedictminus[(samplename, thisCount)] = EtaMinusCount
            plusrow.append(EtaPlusCount)
            minusrow.append(EtaMinusCount)
        etaplusmatrix.append(plusrow)
        etaminusmatrix.append(minusrow)
        nonzerocounts.append(NonZeroCount)
        M = M + NonZeroCount
    return (etaplusmatrix, etaminusmatrix, nonzerocounts, M, N)


def scoreGenes(EtaPlusM, EtaMinusM, binnum, NonZeroCount, M, N):
    '''
    The results matrix contains the EtaPlusFraction, binnum, mutationcount
    and p-value upperbound and lowerbound estimates
    '''
    Lambda = float(M)/float(N)
    pt = PoissonTable(Lambda, int(M))
    output = []
    for i in xrange(len(EtaPlusM)):
        row = []
        etaplus = sum(EtaPlusM[i])
        etaminus = sum(EtaMinusM[i])
        etaplusfrac = float(etaplus)/(etaplus + etaminus) if etaplus > 0 else 0

        # Calculate Upper and Lower bound p-values while handling round off errors
        UpperP = 1.0 - pt.get_cumulative(NonZeroCount[i]-1)
        UpperP = 0.0 if UpperP < 0.0 else  UpperP
        LowerP = 1.0 - pt.get_cumulative(NonZeroCount[i]+1)
        LowerP = 0.0 if LowerP < 0.0 else LowerP

        row.append(binnum)          # Bin Number
        row.append(NonZeroCount[i]) # Total Number of Mutations
        row.append(etaplusfrac)     # EtaPlusFraction
        row.append(UpperP)          # PValue Upperbound
        row.append(LowerP)          # PValue Lowerbound
        row.append(Lambda)          # Lambda value
        output.append(row)
    return output

def calculateBenjaminiHochberg(pval):
    '''
    Benjamini-Hochberg (Independent tests)
    Let tests be H(1), H(2), ..., H(k), ..., H(m)
    Let alpha be the value at which FDR is controlled at
    Sort p-values in ascending order and find the largest k such that
    P(k) <= (k/m) * alpha
    Reject H(1) ... H(k) for that alpha

    Verified against the FDR Calculator at:
    http://sdmproject.com/utilities/?show=FDR
    '''
    pvaluelist = copy.deepcopy(pval)

    # Sort the p-values and check sanity
    if (len(pvaluelist) == 0 or min(pvaluelist) < 0 or max(pvaluelist) > 1):
        print "Length of p-value list: " + str(len(pvaluelist))
        print "Minimum p-value: " + str(min(pvaluelist))
        print "Maximum p-value: " + str(max(pvaluelist))
        raise Exception('p-values invalid')
    pvaluelist.sort()

    m = len(pvaluelist)
    # Find all the P(k)s by assigning alpha values to every single sorted p-value
    currentk = 0
    output = []

    # First calculate the FDR raw graph (will have ups and downs)
    for k in xrange(m):
        output.append(pvaluelist[k]*m/(k+1))

    # Turn into a non-descending function of p-value
    k = m - 1
    while k > 0:
        if output[k-1] > output[k]:
            output[k-1] = output[k]
        k = k - 1

    # Associate the generated FDR values with the unsorted p-values
    # In case of collision, index() returns first value, ensures that
    # the most stringent FDR cutoff value will be assigned
    origoutput = []
    for i in range(0, len(pval)):
        origoutput.append(output[pvaluelist.index(pval[i])])
    return origoutput


######################################################################################
#  Biological Support Classes and Functions
#
######################################################################################

class GeneList(object):
    def __init__(self, binlist):
        self.Genes = {}
        self.GenesByBin = {}
        self.Bins = binlist
        self.Pos = [0 for x in xrange(0,len(binlist.Bins))]
        for x in xrange(0, len(binlist.Bins)):
            self.GenesByBin[x] = []

    def add(self, genename, genelength):
        '''
        If gene does not already exist, add 
        '''
        if genename not in self.Genes.keys():
            binno = self.Bins.getBinNo(genelength)
            self.Genes[genename] = (binno, self.Pos[binno])
            self.GenesByBin[binno].append( (binno, self.Pos[binno]))
            self.Pos[binno] += 1

    def get(self, genename):
        '''
        Returns bin number and index of the gene as tuple
        '''
        return self.Genes[genename]

    def GenesInBin(self, binno):
        return self.GenesByBin[binno]


class BinList(object):
    '''
    Generates and contains list of bins
    '''
    def __init__(self, lower=10, upper=3000, binplusminuspercent=10, minbinwidth=40):
        self.Bins = {}
        origlimitsarray = self.genBinRange(lower, upper, binplusminuspercent, minbinwidth)
        limitsset = set(origlimitsarray)
        limitsarray = list(limitsset)
        limitsarray.sort()
        for i in xrange(0, len(limitsarray)-1):
            self.Bins[i] = (limitsarray[i], limitsarray[i+1]-1) # To prevent bin overlap

    def genBinRange(self, lower, upper, binplusminuspercent, minbinwidth):
        numerator = float(100 + binplusminuspercent)
        denominator = float(100 - binplusminuspercent)
        current = lower
        nextval = current
        binlist = [current]
        while nextval < upper:
            current = nextval
            nextval = int(float(current)/denominator*numerator)
            binlist.append(nextval)
        # Ensure that all the bins are greater than the minimum bin width
        index = 1
        while min([binlist[i+1] - binlist[i] for i in xrange(len(binlist)-1)]) < minbinwidth:
            if( binlist[index] - binlist[index - 1] < minbinwidth):
                binlist.pop(index)
            else:
                index += 1
        return binlist

    def getBinNo(self, genelen):
        for currentkey in self.Bins.keys():
            if self.Bins[currentkey][0] <= genelen and self.Bins[currentkey][1] >= genelen:
                return currentkey
        raise ValueError("Value not contained in the collection of Bins in BinList")


class TumorSample(object):
    '''
    Class to store the mutation count per gene for the patient
    '''
    def __init__(self, sampleid, genelist):
        self.SampleID = sampleid
        self.GeneList = genelist
        self.Mutations = {} # Grouped by bin
        for binno in xrange(len(genelist.Bins.Bins)):
            self.Mutations[binno] = {}

    def addMutation(self, gene):
        bintuple = self.GeneList.get(gene)
        self.Mutations[bintuple[0]][bintuple] = self.Mutations[bintuple[0]].get(bintuple, 0) + 1

    def MutCountsByBin(self,binno):
        '''
        Return dictionary of mutation counts
        '''
        return self.Mutations[binno]

    def RandMutCountsByBin(self,binnum):
        '''
        Return dictionary of randomly permuted mutation counts
        '''
        binsize = len(self.GeneList.GenesByBin[binnum])
        mutcounts = self.Mutations[binnum].values()
        nonzero = random.sample(xrange(binsize), len(mutcounts))
        random.shuffle(nonzero)
        output = {}
        for i in xrange(len(nonzero)):
            output[(binnum, nonzero[i])] = mutcounts[i]
        return output



######################################################################################
#  Main application logic
#
######################################################################################

def Main(argv):
    '''
    Takes user specified parameters over the command line and outputs gene enrichment
    information to a text file (defaults to ./output.txt)
    '''
    # Parse Command Line Arguments
    parser = argparse.ArgumentParser(description='Analyze TCGA data for gene set mutation enrichment for single genes')
    parser.add_argument('-t', '--tissuetype', action='store', dest='cancertype', default="BRCA", help='4 letter abbreviation of tumor type')
    parser.add_argument('-r', '--replicates', action="store", dest="R", default=1000, type=int, help='No. of Replicates used to calculate EtaPlusFraction')
    parser.add_argument('-o', '--outputfile', action='store', dest='outfile', default="output.txt", help='File to output results to')
    parser.add_argument('--version', action='version', version='%(prog)s ' + __version__)
    args = parser.parse_args(argv[1:])
    cancertype = args.cancertype.strip()[0:4].upper()
    testrepsize = args.R
    outfile = args.outfile

    # Create Bins based on Gene lengths
    ExpBinList = BinList(genelenll, genelenul, binpercentwidth, binminwidth)

    print "Analyzing genes of " + cancertype + " for mutation enrichment"
    # Load hugo gene lengths to bin genes of similar lengths together
    AllGenes = GeneList(ExpBinList)
    OOBGenes = []
    f = open(os.path.join(inputdir,'gene_length.txt'), 'r')
    for line in f:
        toks = line.strip('\r').split('\t')
        hugo = toks[0]
        gene_length = int(toks[2]) / 3
        try:
            AllGenes.add(hugo, gene_length)
        except ValueError, err:
            OOBGenes.append(hugo)

    # Load tissue sample mutations
    print "Loading mutation counts"
    tcga = TCGA()
    (mutations, tumor_samples) = tcga.read_tissue_summary(tissue=cancertype, constraints=[\
            ['variant_classification',['MISSENSE', 'NONSENSE', 'READTHROUGH', 'SPLICE']], ['mutation_status',['SOMATIC']]],\
            header = os.path.join(inputdir,'TCGA.'))    
    TumorSamples = {}
    for currentsample in tumor_samples:
        TumorSamples[currentsample] = TumorSample(currentsample, AllGenes)
    for mutation in mutations:
        try:
            TumorSamples[mutation['tumor_sample']].addMutation(mutation['hugo_symbol'])
        except KeyError, err:
            # Gene not in AllGenes due to length constraints
            # TODO: Make a list of the dropped mutations
            pass
   
    EtaPval = {}  # Stores final result as dictionary by tupacc (bin number, gene number)
    for binnum in xrange(len(ExpBinList.Bins)):
        print "Processing bin " + str(binnum+1) + " of " + str(len(ExpBinList.Bins))
        AllGenesInBin = AllGenes.GenesInBin(binnum)
        BinSize = len(AllGenesInBin)
        # Randomly pick the TestGenes to calculate the EtaPlusFraction with
        # (If there are enough genes to choose from)
        if BinSize > testrepsize:
            TestGenes = random.sample(AllGenesInBin, testrepsize)
        else:
            TestGenes = AllGenesInBin

        # Calculate Eta-plus, minus matrices, number of mutated cells for each row, and Poisson parameters
        (EtaPlusM, EtaMinusM, NonZeroCount, M, N) = createMatrices(AllGenesInBin, TestGenes, TumorSamples, binnum)

        # Calculate EtaPlusFraction and p-value range estimates
        ResultsM = scoreGenes(EtaPlusM, EtaMinusM, binnum, NonZeroCount, M, N)

        for row in xrange(BinSize):
            EtaPval[AllGenesInBin[row]] = ResultsM[row]

    # Iterate through entire list of genes and retrieve P-Value list needed to calculate FDR
    GeneNameOrder = []
    PValues = []
    for genename in AllGenes.Genes.keys():
        GeneNameOrder.append(genename)
        PValues.append(EtaPval[AllGenes.Genes[genename]][3])

    print "Calculating FDR"
    FDRValues = calculateBenjaminiHochberg(PValues)

    # First write to a temporary file
    print "Writing results to " + outfile
    tempstring = ''.join(random.choice(string.letters + string.digits) for i in xrange(10))
    outputhandle = open(outfile + "." + tempstring, "w")
    outputhandle.write("Gene\tEtaPlusFrac\tPVal\tFDR\tBinNo\tTotalCount\tLambda\n")
    # Output the final results
    for i in xrange(0, len(GeneNameOrder)):
        genename = GeneNameOrder[i]
        tupacc = AllGenes.Genes[genename]
        outputformat = "\t%8.4f\t%8.3e\t%8.6f\t%8d\t%8d\t%8.3f\n"
        outputhandle.write(str(genename) + outputformat%(EtaPval[tupacc][2], PValues[i], FDRValues[i],
                              EtaPval[tupacc][0]+1,EtaPval[tupacc][1],EtaPval[tupacc][5]))
    outputhandle.close()

    # Rename as atomic operation in case program interrupted before completion
    os.rename(os.path.join(outfile + "." + tempstring), outfile)
    print "Done."

if __name__ == "__main__":
    Main(sys.argv)
