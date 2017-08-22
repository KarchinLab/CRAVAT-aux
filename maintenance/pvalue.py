'''
Second reimplementation of enriched.py that is designed to be as light as possible
'''

import sys, os, random, copy, math
import ConfigParser, argparse
from tcga import TCGA

class PoissonTable:
    def __init__ (self, l, max_x):
        '''
        Creates a Poission dictionary
        '''
        self.max_x = max_x
        self.poisson = {}
        self.e_pow_minus_l = math.exp(-l)
        for x in xrange(1, max_x):
            self.possion[x] = math.pow(l, x) * self.e_pow_minus_l / math.factorial(x)
            
    def get_max_x (self):
        '''
        Returns max_x
        '''
        return self.max_x
    
    def get_pvalue (self, x):
        '''
        Returns the pvalue for x. If x > max_x, it returns -1.
        ''' 
        if self.poisson.has_key(x):
            return self.possion[x]
        else:
            return -1

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


def createEtaPlusMatrix(AllGenes, TestGenes, TumorSamples, TumorSampleOrder, Binnum):
    '''
    NOTE: Need to ensure that TumorSamples is iterated through in the same
          order as in createEtaMinusMatrix
    '''
    output = []
    # Speed up calculation by caching values for each sample
    cachedict = {}
    for thisGene in AllGenes:
        row = []
        for samplename in TumorSampleOrder:
            SampleMut = TumorSamples[samplename].MutCountsByBin(Binnum)
            thisCount = SampleMut.get(thisGene, 0)
            EtaPlusCount = 0
            if (samplename, thisCount) in cachedict:
                EtaPlusCount = cachedict[(samplename, thisCount)]
            else:
                EtaPlusCount = 0
                for thatGene in TestGenes:
                    thatCount = SampleMut.get(thatGene,0)
                    if thisCount > thatCount:
                        EtaPlusCount = EtaPlusCount + 1
                cachedict[(samplename, thisCount)] = EtaPlusCount
            row.append(EtaPlusCount)
        output.append(row)
    return output


def createEtaMinusMatrix(AllGenes, TestGenes, TumorSamples, TumorSampleOrder, Binnum):
    '''
    NOTE: Need to ensure that TumorSamples is iterated through in the same
          order as in createEtaPlusMatrix
    '''
    output = []
    # Speed up calculation by caching values for each sample
    cachedict = {}
    for thisGene in AllGenes:
        row = []
        for samplename in TumorSampleOrder:
            SampleMut = TumorSamples[samplename].MutCountsByBin(Binnum)
            thisCount = SampleMut.get(thisGene, 0)
            EtaMinusCount = 0
            if (samplename, thisCount) in cachedict:
                EtaMinusCount = cachedict[(samplename, thisCount)]
            else:
                EtaMinusCount = 0
                for thatGene in TestGenes:
                    thatCount = SampleMut.get(thatGene,0)
                    if thisCount < thatCount:
                        EtaMinusCount = EtaMinusCount + 1
                cachedict[(samplename, thisCount)] = EtaMinusCount
            row.append(EtaMinusCount)
        output.append(row)
    return output


def createResultsMatrix(EtaPlusM, EtaMinusM):
    '''
    The results matrix contains the generated EtaPlusFraction for each gene as well as
    columns required to do the in place p-value tally/computation and coarse-refine
    drop off of genes that do not need high resolutions
    '''
    output = []
    for i in xrange(len(EtaPlusM)):
        row = []
        etaplus = sum(EtaPlusM[i])
        etaminus = sum(EtaMinusM[i])
        etaplusfrac = float(etaplus)/(etaplus + etaminus) if etaplus > 0 else 0
        row.append(etaplusfrac)
        row.append(0) #LesserThanOrEqualCount
        row.append(0) #TotalCount
        row.append(True) #ContinueFlag
        output.append(row)
    return output


def generateNull(EtaPlusM, EtaMinusM):
    '''
    Generate a path through EtaPlusM and EtaMinusM to create a Null EtaPlusFraction value
    '''
    GeneNum = len(EtaPlusM)
    SampleNum = len(EtaPlusM[0])
    etaplussum = 0
    etaminussum = 0
    for col in xrange(SampleNum):
        row = random.randint(0, GeneNum - 1)
        etaplussum = etaplussum + EtaPlusM[row][col]
        etaminussum = etaminussum + EtaMinusM[row][col]
    return float(etaplussum)/(etaplussum + etaminussum) if etaplussum > 0 else 0


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


def Main(argv):
    '''
    Main application logic
    '''

    # Just hard code for now...
    cancertype = "GBM"
    inputdir = "database"
    genelenll = 10
    genelenul = 3000
    binpercentwidth = 10
    binminwidth = 40
    maxiteration = 10000000
    miniteration = 100

    # Initialize the Bins
    print "Initializing the bins"
    ExpBinList = BinList(genelenll, genelenul, binpercentwidth, binminwidth)

    # Load all hugo genes information
    print "Loading hugo gene information"
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
    print "Loading tissue information"
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

    # Stores final result as dictionary by tupacc (bin number, gene number)
    EtaPval = {}
    
    # Calculate Etaplus fraction and p-value counts for all genes of the bin
    for binnum in xrange(len(ExpBinList.Bins)):

        print "Processing bin number " + str(binnum)
        # Retrieve a list of All Genes
        AllGenesInBin = AllGenes.GenesInBin(binnum)

        # Randomly pick the TestGenes to calculate the EtaPlusFraction with
        BinSize = len(AllGenesInBin)
        if BinSize >= 1000:
            TestGenes = random.sample(AllGenesInBin, 1000)
        else:
            TestGenes = AllGenesInBin

        # Ensures that order of the TumorSamples is consistent for both Eta-plus and Eta-minus matrices
        TumorSampleOrder = TumorSamples.keys()

        # Calculate Eta-plus matrix
        EtaPlusM = createEtaPlusMatrix(AllGenesInBin, TestGenes, TumorSamples, TumorSampleOrder, binnum)

        # Calculate Eta-minus matrix
        EtaMinusM = createEtaMinusMatrix(AllGenesInBin, TestGenes, TumorSamples, TumorSampleOrder, binnum)
        
        # Create the results matrix (EtaPlusFraction, LesserThanEqualToCount, TotalCount, EvaluateFlag)
        ResultsM = createResultsMatrix(EtaPlusM, EtaMinusM)
        
        # Set ModulusValue to 1000
        ModVal = miniteration
        Counter = 1
        while Counter <= ModVal:
            # When a checkpoint has been reached, drop all genes that do not require higher resolution
            if Counter == ModVal:
                SomeLeftFlag = False
                for row in xrange(BinSize):
                    # If LesserThanEqualToCount > 0 set EvaluateFlag to False
                    if ResultsM[row][1] > 0:
                        ResultsM[row][3] = False
                    else:
                        SomeLeftFlag = True
                        # If highest resolution reached, add pseudocount of 1 to both
                        # LesserThanEqualToCount and TotalCount
                        if Counter == maxiteration:
                            ResultsM[row][1] = ResultsM[row][1] + 1
                            ResultsM[row][2] = ResultsM[row][2] + 1

                # Continue Iterating if highest resolution is not reached and
                # there are samples left with 0 entries in LesserThanEqualToCount column
                if SomeLeftFlag == True and Counter < maxiteration:
                        ModVal = ModVal * 10
            else:
                # Generate Null replicate score using Etaplus and Etaminus Matrices
                nullscore = generateNull(EtaPlusM, EtaMinusM)

                for row in xrange(BinSize):
                    # If EvaluateFlag == True:
                    if ResultsM[row][3] == True:
                        # If Null replicate score <= Etaplusfraction value for row
                        if ResultsM[row][0] <= nullscore:
                            # Increment LesserThanEqualToCount
                            ResultsM[row][1] = ResultsM[row][1] + 1
                        # Increment TotalCount
                        ResultsM[row][2] = ResultsM[row][2] + 1

            if Counter % 100000 == 0:
                print "Bin " + str(binnum) + "\t" + str(Counter) + " iterations completed"
            Counter = Counter + 1


        # Add all the genes in the matrix to the EtaPVal dictionary
        # NOTE: Third row is to store the accuracy (number of decimal places) that the result should be printed by
        for row in xrange(BinSize):
            EtaPval[AllGenesInBin[row]] = (ResultsM[row][0], float(ResultsM[row][1])/ResultsM[row][2], int(math.log10(ResultsM[row][2])))

    # Iterate through entire list of genes and retrieve P-Value list needed to calculate FDR
    GeneNameOrder = []
    PValues = []
    for genename in AllGenes.Genes.keys():
        GeneNameOrder.append(genename)
        tupacc = AllGenes.Genes[genename]
        PValues.append(EtaPval[tupacc][1])

    FDRValues = calculateBenjaminiHochberg(PValues)

    outputhandle = open("output.txt", "w")    
    # Output the final results
    for i in xrange(0, len(GeneNameOrder)):
        # TODO: Implement display by accuracy
        genename = GeneNameOrder[i]
        tupacc = AllGenes.Genes[genename]
        outputhandle.write(str(genename) + "\t%8.4f\t%8.5f\t%8.5f"%(EtaPval[tupacc][0], PValues[i], FDRValues[i]) + "\n")

    outputhandle.close()

if __name__ == "__main__":
    Main(sys.argv)
