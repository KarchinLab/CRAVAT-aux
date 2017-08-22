#!/usr/bin/env python
'''
Tests for gene mutation enrichment in TCGA data
Algorithm developed by Dr. Donald Geman

Tested on Mac OS 10.7.3

REQUIREMENTS
- python 2.7
- os environment that supports multiprocessing (does not multiprocess in python console mode)

CHANGES COMPARED TO PREVIOUS VERSION
- Changed from strict formula based approach to a more object oriented approach for better readability
- Removed previously tested ranksum methods
- Removed using all genes as background for generalized support of gene set replicates
- Removed precomputed eta_plus_frac file because not possible with gene sets
- Added settings.ini for configuration settings (should be in the same folder as this file)
- Bins are automatically generated using gene length and bin parameters in settings.ini
- Converted into a command line utility
- Added ability to take advantage of multiple processors

TODO: A way to specify the output data file (now defaults to Output.txt in the output_dir)
TODO: Optimize memory usage
TODO: Fix quirk with default config file settings not working
'''
__author__ = "Rick Kim, Wing Chung Wong"
__copyright__ = "Copyright 2012, Johns Hopkins University"
__credits__ = ["Donald Geman", "Rachel Karchin", "Rick Kim", "Michael Ryan", "Wing Chung Wong"]
__license__ = "JHU"
__version__ = "0.5b"
__maintainer__ = "Wing Chung Wong"
__email__ = "awong@insilico.us.com"
__status__ = "Under active development"

import os, sys
import random, string
import math
import copy
import ConfigParser, argparse
import multiprocessing

from tcga import TCGA


#############################################################################################
#
#  Foundation Classes Section
#
#  Basic Classes to keep things readable and organized
#
#############################################################################################


class Bin(object):
    '''
    Immutable Bin object whose bounds cannot be changed once initialized
    '''
    def __init__(self, lowerlim, upperlim, binnum):
        super(Bin, self).__setattr__('UpperLimit', upperlim)
        super(Bin, self).__setattr__('LowerLimit', lowerlim)
        super(Bin, self).__setattr__('BinNumber', binnum)

    def __setattr__(self, *args):
        raise TypeError("Cannot modify immutable instance")

    __delattr__ = __setattr__

    def contains(self, genelen):
        '''
        Inclusive range
        '''
        if ( (genelen >= self.LowerLimit) and (genelen <= self.UpperLimit) ):
            return True
        else:
            return False

    def getUpperLimit(self):
        return self.UpperLimit

    def getLowerLimit(self):
        return self.LowerLimit

    def getBinNo(self):
        return self.BinNumber


class BinList(object):
    '''
    Generates and contains list of bins, emulates dictionary interface
    '''
    def __init__(self, lower=10, upper=3000, binplusminuspercent=10, minbinwidth=40):
        self.__Bins = {}
        origlimitsarray = self.genBinRange(lower, upper, binplusminuspercent, minbinwidth)
        limitsset = set(origlimitsarray)
        limitsarray = list(limitsset)
        limitsarray.sort()
        for i in xrange(0, len(limitsarray)-1):
            self.__Bins[i] = Bin(limitsarray[i], limitsarray[i+1]-1, i) # To prevent bin overlap

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

    def getBinNoByGeneLen(self, genelen):
        for currentkey in self.__Bins.keys():
            if( self.__Bins[currentkey].contains(genelen) ):
                return self.__Bins[currentkey].getBinNo()
        raise ValueError("Value not contained in the collection of Bins in BinList")

    def __contains__(self, other):
        return self.__Bins.__contains__(other)

    def keys(self):
        return self.__Bins.keys()

    def __setitem__(self, key, value):
        if( isinstance(value, Bin)):
            self.__Bins[key] = value
        else:
            raise TypeError("BinList should only contain Bin objects")

    def __getitem__(self, key):
        return self.Bins[key]

    def __delitem__(self, key):
        del self.Bins[key]

    def __len__(self):
        return len(self.__Bins)


class Callable:
    '''
    Wrapper that allows classes to contain static methods without generating unbound
    method error
    '''
    def __init__(self, anycallable):
        self.__call__ = anycallable


class Distribution():
    '''
    Static class to provide p-value and fdr computations
    '''

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

    def calculatePValue(value, othervalues):
        '''
        Calculates empirical p-value given the value and a list of other values in the distribution

        Since the more counts, the more enriched it is, we are looking at the proportion of eta_plus_fraction
        greater than or equal to the value in question.
        (If we use strictly greater than, it would tend to underestimate the p-value whenever there are many
        duplicate values at the cutoff, causing significant problems when the counts are all zeroes.)
        '''
        greaterthanequals = 0
        for val in othervalues:
            greaterthanequals += 1 if val >= value else 0
        # Avoid returning p-value of zero
        return float(greaterthanequals+1)/(len(othervalues) + 1)

    calculatePValue = Callable(calculatePValue)
    calculateBenjaminiHochberg = Callable(calculateBenjaminiHochberg)




class FileParser():
    '''
    File Parser object that contains different methods for parsing different types of files
    '''

    def __init__(self, loadedgeneslist):
        '''
        NOTE: loadedgeneslist is a genes list containing all the binned genes that are looked at
              in the analysis. Genes not belonging to the list will be ignored and not loaded
        '''
        self.__GenesList = loadedgeneslist

    def parseAnnotatedGeneSetFile(self, genesetsfile):
        '''
        Parse annotated Geneset input files of the following format:
        NAME:blahblah
        DESC:blahblahblah
        MEMB:HUGO1,HUGO2,HUGO3...

        NOTE: All lines are compulsory, there cannot be empty rows
        '''
        testingGeneSets = []
        f = open(genesetsfile, 'r')
        NAMELine = f.readline()
        DESCLine = f.readline()
        MEMBLine = f.readline()
        while NAMELine != "" and DESCLine != "" and MEMBLine != "":
            if NAMELine[0:5] != "NAME:" or DESCLine[0:5] != "DESC:" or MEMBLine[0:5] != "MEMB:":
                raise IOError("Invalid annotated gene set file format" +\
                              "(Keywords NAME, DESC and MEMB must be capitalized; Cannot have empty rows anywhere in the file)")
                break
    
            thisgeneset = GeneSet(NAMELine[5:].strip(), DESCLine[5:].strip())
            toks = MEMBLine[5:].strip().split(',')
            for tok in toks:
                try:
                    thisgeneset.addGene(self.__GenesList[tok.strip()])
                except KeyError, err:
                    # Gene not in AllGenes due to length constraints, add it to the dropped genes list
                    thisgeneset.addDroppedGene(tok.strip())              
            
            if(len(thisgeneset) > 0):
                testingGeneSets.append(thisgeneset)
                
            NAMELine = f.readline()
            DESCLine = f.readline()
            MEMBLine = f.readline()
            
        return testingGeneSets

    def parseSimpleGeneSetFile(self, genesetsfile):
        '''
        Parse non-annotated Geneset input files of the following format:
        HUGO1,HUGO2,HUGO3...
        '''
        testingGeneSets = []
        f = open(genesetsfile, 'r')
        for line in f:
            toks = line.strip('\r').split(',')
            thisgeneset = GeneSet()
            for tok in toks:
                try:
                    thisgeneset.addGene(self.__GenesList[tok.strip()])
                except KeyError, err:
                    # Gene not in AllGenes due to length constraints, add it to the dropped genes list
                    thisgeneset.addDroppedGene(tok.strip())

            if(len(thisgeneset) > 0):
                testingGeneSets.append(thisgeneset)
        return testingGeneSets


    def parseGeneSetFile(self, genesetsfile):
        '''
        Checks to see if the file is present and readable, then uses the correct
        function to parse the file depending on the content type
        '''
        if(genesetsfile == "" or os.path.exists(genesetsfile) == False):
            print "Please specify input file for GeneSets to be tested"
            sys.exit()
        else:
            # Determine which file parsing 
            f = open(genesetsfile, 'r')
            line = f.readline()
            f.close()
            if line[0:5] == "NAME:":
                return self.parseAnnotatedGeneSetFile(genesetsfile)
            else:
                return self.parseSimpleGeneSetFile(genesetsfile)


class Gene(object):
    '''
    Immutable Gene object containing Hugo Symbol, Gene length and Bin Number
    '''
    def __init__(self, hugo, genelen, binnum):
        super(Gene, self).__setattr__("HugoSymbol", hugo)
        super(Gene, self).__setattr__("GeneLength", genelen)
        super(Gene, self).__setattr__("BinNumber", binnum)

    def __setattr__(self, *args):
        raise TypeError("Cannot modify immutable instance")

    __delattr__ = __setattr__

    def __eq__(self, other):
        if isinstance(other, Gene):
            return (self.HugoSymbol == other.getHugo()) and \
                   (self.GeneLength == other.getGeneLength()) and \
                    (self.BinNumber == other.getBinNo())
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def getHugo(self):
        return self.HugoSymbol

    def getGeneLength(self):
        return self.GeneLength

    def getBinNo(self):
        return self.BinNumber



class GeneList(object):
    '''
    Class to contain a list of genes to look up by bin no or Hugo ID and
    to generate randomly chosen replicates for a Gene Set
    '''

    def __init__(self):
        self.__GenesByBin = {}
        self.__GenesByHugo = {}

    def getGenesByBinNo(self, binnum):
        # We do not throw exceptions here because we may legitimately
        # not have any genes for a particular bin number in the GeneList
        return self.__GenesByBin.get(binnum, [])

    def addGene(self, newgene):
        if( isinstance(newgene, Gene)):
            if( self.__GenesByHugo.has_key(newgene.getHugo())):
                raise ValueError('GeneList already contains this gene')
            else:
                if( self.__GenesByBin.has_key(newgene.getBinNo())):
                    self.__GenesByBin[newgene.getBinNo()].append(newgene)
                else:
                    self.__GenesByBin[newgene.getBinNo()] = [newgene]
                self.__GenesByHugo[newgene.getHugo()] = newgene
        else:
            raise TypeError("GeneList should only contain Gene objects")

    def keys(self):
        return self.__GenesByHugo.keys()

    def __getitem__(self, key):
        return self.__GenesByHugo[key]

    def __delitem__(self, key):
        g = self.__GenesByHugo[key]
        self.__GenesByBin[g.getBinNo()].remove(g)
        del self.__GenesByHugo[key]

    def __len__(self):
        return len(self.__GenesByHugo)

    def getReplicate(self, gset, R=1):
        '''
        Generator returning randomly generated replicates for a given gset that does not
        contain any of the members of this original gset
        '''
        if(isinstance(gset, GeneSet)):
            for r in xrange(R):
                binnos = gset.getBinNos()
                thisrep = GeneSet()
                for binno in binnos:
                    trial = 1
                    randgene = self.getRandomGeneForBin(binno)
                    while(randgene in gset):
                        randgene = self.getRandomGeneForBin(binno)
                        trial += 1
                        if( trial > 100):
                            raise Error('Tried 100 times, cannot find gene in bin ' + str(binno) + ' not in original geneset')
                    thisrep.addGene(self.getRandomGeneForBin(binno))
                yield thisrep
        else:
            raise TypeError('GeneList.getReplicate() only accepts GeneSet as input')


    def getRandomGeneForBin(self, binno):
        '''
        Returns a single randomly chosen gene from a given bin
        '''
        if( self.__GenesByBin.get(binno, []) != []):
            return random.choice(self.__GenesByBin[binno])
        else:
            raise ValueError("This GeneList does not have genes for bin " + str(binno))


class GeneSet(object):
    '''
    Class to contain a set of one or more genes. Differs from GeneList in the sense that
    these genes should have some sort of biological relationship/purpose:
    i.e. May be part of a pathway, or may all be cancer-related etc.

    These GeneSets are tested to see if they contain significantly more mutations compared
    to GeneSets of genes of similar lengths
    '''
    def __init__(self, gsid="", gsdesc=""):
        self.__Genes = set()
        self.__Dropped = set()
        self.__ID = gsid
        self.__Desc = gsdesc

    def addGene(self, newgene):
        if( isinstance(newgene, Gene) ):
            self.__Genes.add(newgene)
        else:
            raise TypeError('GeneSet can only contain objects of class Gene')

    def addDroppedGene(self, hugoid):
        if( isinstance(hugoid, basestring)):
            self.__Dropped.add(hugoid)
        else:
            raise TypeError('Dropped genes can only contain strings')

    def __len__(self):
        return len(self.__Genes)

    def getBinNos(self):
        '''
        Returns a list of Bin No.s that is useful for generating gene set replicates
        '''
        output = []
        for gene in self.__Genes:
            output.append(gene.getBinNo())
        output.sort()
        return output

    def getDroppedGenes(self):
        '''
        Outputs a list of genes that are dropped because of size constraints
        '''
        unsortedlist = list(self.__Dropped)
        unsortedlist.sort()
        output = ""
        for i in xrange(len(unsortedlist)):
            output += unsortedlist[i] + ","
        return str(output[0:-1])

    def __repr__(self):
        '''
        Outputs the gene set in python list format sorted alphanumerically
        '''
        unsortedlist = [g.getHugo().strip() for g in self.__Genes]
        unsortedlist.sort()
        output = ""
        for i in xrange(len(unsortedlist)):
            output += unsortedlist[i] + ","
        return str(output[0:-1])

    def __contains__(self, item):
        if(isinstance(item, Gene)):
            return item in self.__Genes
        else:
            raise TypeError('GeneSet only works with Gene objects')

    def contains(self, item):
        return self.__contains__(item)

    def __iter__(self):
        return self.__Genes.__iter__()

    def getMutationCountForSample(self, sample):
        if( isinstance(sample, TumorSample)):
            totalcounts = 0
            for gene in self.__Genes:
                totalcounts += sample.getMutationCountsForGene(gene)
            return totalcounts
        else:
            raise TypeError('GeneSet.getMutationCountForSample only works with TumorSample object as input')

    def getID(self):
        return self.__ID

    def getDescription(self):
        return self.__Desc


class TumorSample(object):
    '''
    Class to store the mutation count per gene for the patient
    '''
    def __init__(self, sampleid):
        self.__SampleID = sampleid
        self.__MutationCountByGene = {}

    def addMutation(self, gene):
        if( isinstance(gene, Gene)):
            self.__MutationCountByGene[gene] = self.__MutationCountByGene.get(gene, 0) + 1
        else:
            raise TypeError('TumorSample expects Gene objects, not strings of Hugo symbols')
        
    def getMutationCountsForGene(self, gene):
        if( isinstance(gene, Gene)):
            return self.__MutationCountByGene.get(gene, 0)
        else:
            raise TypeError('TumorSample expects Gene objects, not strings of Hugo symbols')

    def getID(self):
        return self.__SampleID

    def __repr__(self):
        return str(self.__MutationCountByGene.values())


#############################################################################################
#
#  Sign Test Section
#
#  Functions to support the formulas and multiprocessing
#
#############################################################################################

def STThreadDelegator(genesets, genelist, tumorsamples, r, distsize):
    '''
    Generator function that delegates workload to the worker threads in the pool
    Should work because genelist, r, distsize and tumorsamples are not written to
    '''
    for testset in genesets:
        yield (testset, genelist, tumorsamples, r, distsize)

def STThreadWorkerOptimized(arg):
    '''
    Using a fixed set of test replicates R, score testset and a population of other replicates to get p-value
    (Speed Optimized)
    '''
    testset = arg[0]
    genelist = arg[1]
    tumorsamples = arg[2]
    r = arg[3]
    distsize = arg[4]
    
    # SPEED OPTIMIZATION: Convert into fixed counts
    FixedSampleOrder = tumorsamples.keys()
    FixedSampleOrder.sort()
    tumornum = len(FixedSampleOrder)
    FixedTestReplicates = list(genelist.getReplicate(testset, r))
    FixedDistReplicates = list(genelist.getReplicate(testset, distsize))
    FixedDistReplicates.insert(0, testset)

    # Order is important in the following for loops
    testreps = []
    for index in xrange(tumornum):
        testreps.append([FixedTestReplicates[i].getMutationCountForSample(tumorsamples[FixedSampleOrder[index]]) for i in xrange(len(FixedTestReplicates))])   

    reps = []
    for repset in FixedDistReplicates:
        reps.append([repset.getMutationCountForSample(tumorsamples[FixedSampleOrder[index]]) for index in xrange(tumornum)])

    etaplusfracarray = []
    cachedresults = {} # SPEED OPTIMIZATION: Cache computed eta_plus_frac value for given sample and mutation count
    for repindex in xrange(len(reps)):
        etaplus = 0
        etaminus = 0
        for tumorindex in xrange(tumornum):
            rep = reps[repindex][tumorindex]
            sampetaplus = 0
            sampetaminus = 0
            if cachedresults.has_key((tumorindex,rep)):
                sampetaplus = cachedresults[(tumorindex,rep)][0]
                sampetaminus = cachedresults[(tumorindex,rep)][1]
            else:
                for testrep in testreps[tumorindex]:
                    sampetaplus += 1 if testrep < rep else 0
                    sampetaminus += 1 if testrep > rep else 0
                cachedresults[(tumorindex,rep)] = (sampetaplus, sampetaminus)
            etaplus += sampetaplus
            etaminus += sampetaminus
        etaplusfrac = 0 if etaplus == 0 else (float(etaplus)/(etaminus+etaplus))
        etaplusfracarray.append(etaplusfrac)

    pval = Distribution.calculatePValue(etaplusfracarray[0], etaplusfracarray[1:])


    # INCREASE RESOLUTION FOR p-values at the bottom most value if we are using less than
    # 100 000 replicates
    if( r < 100000):
        smallestpossiblevalue = 2.0/(r+1)
        if round(pval, 10) <= round(smallestpossiblevalue, 10):
            # If there is only one observed sample, repeat the above analysis using 100,000 replicates
            distsize = 100000
            FixedDistReplicates = list(genelist.getReplicate(testset, distsize))
            FixedDistReplicates.insert(0, testset)

            reps = []
            for repset in FixedDistReplicates:
                reps.append([repset.getMutationCountForSample(tumorsamples[FixedSampleOrder[index]]) for index in xrange(tumornum)])

            etaplusfracarray = []
            cachedresults = {}
            for repindex in xrange(len(reps)):
                etaplus = 0
                etaminus = 0
                for tumorindex in xrange(tumornum):
                    rep = reps[repindex][tumorindex]
                    sampetaplus = 0
                    sampetaminus = 0
                    if cachedresults.has_key((tumorindex,rep)):
                        sampetaplus = cachedresults[(tumorindex,rep)][0]
                        sampetaminus = cachedresults[(tumorindex,rep)][1]
                    else:
                        for testrep in testreps[tumorindex]:
                            sampetaplus += 1 if testrep < rep else 0
                            sampetaminus += 1 if testrep > rep else 0
                        cachedresults[(tumorindex,rep)] = (sampetaplus, sampetaminus)
                    etaplus += sampetaplus
                    etaminus += sampetaminus
                etaplusfrac = 0 if etaplus == 0 else (float(etaplus)/(etaminus+etaplus))
                etaplusfracarray.append(etaplusfrac)

            pval = Distribution.calculatePValue(etaplusfracarray[0], etaplusfracarray[1:])

    # To control how many decimal places to display the p-value up to
    accuracy = math.floor(math.log10(distsize + 1))
    
    return (testset, etaplusfracarray[0], pval, accuracy)


# Obsolete Function (Slow)
def STThreadWorker(arg):
    '''
    Thread worker function that calculates the etaplusfrac and pvalue for a geneset
    (Fully randomized version)
    '''
    testset = arg[0]
    genelist = arg[1]
    tumorsamples = arg[2]
    r = arg[3]
    distsize = arg[4]
    etaplusfrac = calculateEtaPlusFraction(testset, r, genelist, tumorsamples)
    others = []
    for i in xrange(distsize):
        repset = list(genelist.getReplicate(testset))[0]
        others.append(calculateEtaPlusFraction(repset, r, genelist, tumorsamples))
    pval = Distribution.calculatePValue(etaplusfrac, others)
    return (testset, etaplusfrac, pval)

# Obsolete Function (Slow)
def calculateEtaPlusFraction(geneset, r, genelist, tumorsamples):
    '''
    NOTE: Summation is reversed compared to the original formula for convenience
          (The original sums over samples first before replicates)
    '''
    # GeneList.getReplicate() is a generator function, this makes it into an actual list
    FixedReplicates = list(genelist.getReplicate(geneset, r))
    
    eta_plus = 0
    eta_minus = 0
    for sample in tumorsamples.values():
        origscore = geneset.getMutationCountForSample(sample)
        eta_plus_for_sample = 0
        eta_minus_for_sample = 0
        for rep in FixedReplicates:
            replicatescore = rep.getMutationCountForSample(sample)
            eta_plus_for_sample += 1 if origscore > replicatescore else 0
            eta_minus_for_sample += 1 if origscore < replicatescore else 0
        eta_plus += eta_plus_for_sample
        eta_minus += eta_minus_for_sample
    
    # Avoid division by zero when both eta_plus and eta_minus are zero
    if(eta_plus == 0):
        return float(0)
    else:
        return float(eta_plus)/float(eta_plus + eta_minus)


#############################################################################################
#
#  Main Section
#
#  Configuration settings and defaults, commandline arguments, file input and output, and
#  overall program flow
#
#############################################################################################

# Hard-coded defaults for settings.ini parameters
Defaults = {'MutationTest':{
                    'input_dir':'.',
                    'output_dir':'.',
                    'gene_length_lower_limit':'10',
                    'gene_length_upper_limit':'3000',
                    'bin_percent_width':'10',
                    'bin_min_width':'40'
                    }
                }

def Main(argv):
    '''
    A Main function to provide command line and configuration file interface for the user
    '''
    # Parse Command Line Arguments
    parser = argparse.ArgumentParser(description='Analyze TCGA data for gene set mutation enrichment.')

    scoringsubject = parser.add_mutually_exclusive_group()
    scoringsubject.add_argument('-i', '--input', action='store', dest='genesetsfile', default="", help='input file containing GeneSets to be tested')
    scoringsubject.add_argument('-a', '--alist', action='store_true', dest='scoreAlist', default=False, help='score the A-list genes')
    scoringsubject.add_argument('-b', '--blist', action='store_true', dest='scoreBlist', default=False, help='score the B-list genes')
    scoringsubject.add_argument('-ab', '--ablist', action='store_true', dest='scoreABlist', default=False, help='score both A and B list genes')
    scoringsubject.add_argument('-all', '--allhugo', action='store_true', dest='scoreAllhugo', default=False, help='score all hugo genes')
    
    parser.add_argument('-t', '--tissuetype', action='store', dest='cancertype', default="BRCA", help='four letter abbreviation of tumor type')
    parser.add_argument('-r', '--replicates', action="store", dest="R", default=100, type=int, help='number of Replicates to test per gene set')
    parser.add_argument('-d', '--distribution', action="store", dest="distsize", default=100, type=int, help='number of Replicates to use for computing empirical p-value')
   
    method = parser.add_mutually_exclusive_group()
    method.add_argument('--signtest', action="store_true", default=True, help='Compute sign test of gene set')

    parser.add_argument('-p', '--processors', action='store', dest='processors',default=1, type=int, help='number of Processors to use')
    parser.add_argument('-v','--verbose', action='store_true', help='output all intermediate messages')
    parser.add_argument('--version', action='version', version='%(prog)s ' + __version__)

    args = parser.parse_args(argv[1:])
    cancertype = args.cancertype.strip()[0:4].upper()
    distsize = args.distsize

    # Read Configuration file settings
    Config = ConfigParser.ConfigParser(Defaults)
    
    # Throw error message if configuration file is absent but continue
    settingspath = os.path.join(os.path.dirname(os.path.realpath(__file__)),'settings.ini')
    if(os.path.exists(settingspath)):
        Config.read(settingspath)
    else:
        print "Configuration file 'settings.ini' not found"
        sys.exit()
        
    
    inputdir = Config.get('MutationTest','input_dir') # TCGA preprocessed data input directory
    outputdir = Config.get('MutationTest', 'output_dir')
    genelenll = Config.getint('MutationTest', 'gene_length_lower_limit')
    genelenul = Config.getint('MutationTest', 'gene_length_upper_limit')
    binpercentwidth = Config.getint('MutationTest', 'bin_percent_width')
    binminwidth = Config.getint('MutationTest', 'bin_min_width')

    # Initialize the Bins
    ExpBinList = BinList(genelenll, genelenul, binpercentwidth, binminwidth)

    # Load all hugo genes information
    AllGenes = GeneList()
    OOBGenes = []
    f = open(os.path.join(inputdir,'gene_length.txt'), 'r')
    for line in f:
        toks = line.strip('\r').split('\t')
        hugo = toks[0]
        gene_length = int(toks[2]) / 3
        try:
            bin_num = ExpBinList.getBinNoByGeneLen(gene_length)
            AllGenes.addGene(Gene(hugo, gene_length, bin_num))
        except ValueError, err:
            OOBGenes.append(hugo)

    # Load gene lists
    AListGenes = GeneList()
    BListGenes = GeneList()
    BGGenes = GeneList()
    f = open(os.path.join(inputdir, 'a and b list.txt'), 'r')
    for line in f:
        toks = line.strip('\n').split('\t')
        hugo = toks[0]
        list_type = toks[-1]
        if hugo in AllGenes.keys():
            if list_type == 'A':
                AListGenes.addGene(AllGenes[hugo])
            elif list_type == 'B':
                BListGenes.addGene(AllGenes[hugo])
    f.close()
    for hugo in AllGenes.keys():
        if not hugo in AListGenes.keys() and not hugo in BListGenes.keys():
            BGGenes.addGene(AllGenes[hugo])

    # Get the GeneSets to be scored depending on user specification
    testingGeneSets = []
    if args.scoreAlist == True or args.scoreABlist == True:
        for genekey in AListGenes.keys():
            gset = GeneSet(genekey, "Vogelstein A List")
            gset.addGene(AListGenes[genekey])
            testingGeneSets.append(gset)
    elif args.scoreBlist == True or args.scoreABlist == True:
        for genekey in BListGenes.keys():
            gset = GeneSet(genekey, "Vogelstein B List")
            gset.addGene(BListGenes[genekey])
            testingGeneSets.append(gset)
    elif args.scoreAllhugo == True:
        for genekey in AllGenes.keys():
            gset = GeneSet(genekey, "All Hugo genes")
            gset.addGene(AllGenes[genekey])
            testingGeneSets.append(gset)
    else:
        fp = FileParser(AllGenes)
        testingGeneSets = fp.parseGeneSetFile(args.genesetsfile)

    # Load tissue sample mutations
    tcga = TCGA()
    (mutations, tumor_samples) = tcga.read_tissue_summary(tissue=cancertype, constraints=[\
            ['variant_classification',['MISSENSE', 'NONSENSE', 'READTHROUGH', 'SPLICE']]],\
            header = os.path.join(inputdir,'TCGA.'))    
    TumorSamples = {}
    for currentsample in tumor_samples:
        TumorSamples[currentsample] = TumorSample(currentsample)
    for mutation in mutations:
        try:
            TumorSamples[mutation['tumor_sample']].addMutation(AllGenes[mutation['hugo_symbol']])
        except KeyError, err:
            # Gene not in AllGenes due to length constraints
            # TODO: Make a list of the dropped mutations
            pass


    if(args.verbose == True):
        print "Number of GeneSets to be tested: " + str(len(testingGeneSets))
        print "A List (Vogelstein rules): " + str(len(AListGenes.keys()))
        print "B List (Vogelstein rules): " + str(len(BListGenes.keys()))
        print "Background List: " + str(len(BGGenes.keys()))
        print "Dropped Genes due to length constraints: " + str(len(OOBGenes))
        print "Tumor Type: " + cancertype
        print "Number of Samples: " + str(len(TumorSamples))
        print "Number of Testing Replicates: " + str(args.R)
        print "Number of Distribution Replicates:" + str(distsize)


    # Fill in logic for different scoring algorithms here
    if( args.signtest == True):
        if(args.verbose == True):
            print "Calculating eta_plus_fraction and pvalues for each Gene Set"

        exptestsets = []
        etavalues = []
        pvalues = []
        accvalues = []
        
        # Part where multiprocessing is done
        pool = multiprocessing.Pool(args.processors)
        it = pool.imap(STThreadWorkerOptimized, STThreadDelegator(testingGeneSets, AllGenes, TumorSamples, args.R, distsize))
        computedindex = 0
        while(True):
            try:
                test = it.next()
                exptestsets.append(test[0])
                etavalues.append(test[1])
                pvalues.append(test[2])
                accvalues.append(test[3])
                computedindex += 1
                if(args.verbose == True):
                    print "Computed sign test and p-value for " + str(computedindex) + " of " + str(len(testingGeneSets)) + " Gene Sets"
            except StopIteration:
                break

        # FDR is computed again on a single core
        if(args.verbose == True):
            print "Calculating Benjamini Hochberg FDR alpha values"
        fdrvalues = Distribution.calculateBenjaminiHochberg(pvalues)

        if(args.verbose == True):
            print "Outputting results to file"

        # First write to a temporary file
        tempstring = ''.join(random.choice(string.letters + string.digits) for i in xrange(10))
        f = open(os.path.join(outputdir, "output.txt." + tempstring),"w")
        f.write('ID\tDescription\tGenes\tDropped_Genes\tNum_of_Genes\teta_plus_frac\tp-value\tBH_alpha\n')
        for i in xrange(len(exptestsets)):
            outputformat = '\t%8.3f\t%8.' + str(int(accvalues[i])) + 'f\t%8.2f' # To output the p-value to the correct decimal place based on the number of distribution replicates used
            f.write( exptestsets[i].getID() + "\t" + exptestsets[i].getDescription() + "\t" + str(exptestsets[i]) + "\t" + exptestsets[i].getDroppedGenes() + "\t" + str(len(exptestsets[i])) + outputformat%(etavalues[i], pvalues[i], fdrvalues[i]) + "\n")
        f.close()

        # Rename as atomic operation in case program interrupted before completion
        os.rename(os.path.join(outputdir, "output.txt." + tempstring), os.path.join(outputdir, "output.txt"))
        

if __name__ == "__main__":
    Main(sys.argv)
