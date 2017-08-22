#!/usr/bin/env python

'''
Quick script to parse the pathways tsv file from PharmGKB to obtain a list of
pathways for which to score using Gene enrichment analysis
'''

import sys


class PathwayGeneSet(object):
    '''
    This class contains gene sets parsed from PharmGKB pathways tsv file
    '''
    def __init__(self, pathwayid):
        self._pathwayID = pathwayid
        self._genes = set()

    def setDesc(self, desc):
        self._desc = desc

    def getID(self):
        return self._pathwayID

    def getDesc(self):
        return self._desc

    def addGene(self, hugoid):
        self._genes.add(hugoid)

    def getGeneList(self):
        output = list(self._genes)
        output.sort()
        return output

    def __len__(self):
        return len(self._genes)

    def __repr__(self):
        return self._pathwayID + " (" + str(len(self._genes)) + " genes)"

    def toString(self):
        genelist = self.getGeneList()
        output = ''
        for gene in genelist:
            output += gene + ","
        return output[0:-1]

class TSVParser(object):
    '''
    This class parses a tsv file and returns pathway gene sets in a generator function
    '''

    def __init__(self, filepath):
        self._fileLocation = filepath
        self._lineReader = LineReader()

    def getGeneSets(self):
        '''
        Generator function that parses genesets out of the tsv file
        '''
        f = open(self._fileLocation, 'r')
        lr = self._lineReader
        try:
            # Parse the file and yield results incrementally
            line = f.readline()
            while True:

                # Skip to the first definition
                while (lr.isDefLine(line) == False) and (line != ""):
                    line = f.readline()

                # Exit the loop if it is the end of the file
                if line == "":
                    break

                # Now that we are at a definition line, start a new PathwayGeneSet object
                currentID = lr.getPathwayID(line)
                currentDesc = lr.getPathwayDesc(line)
                currentGS = PathwayGeneSet(currentID)
                currentGS.setDesc(currentDesc)

                # Load all the genes until the connection header line
                while lr.isConnectionHeader(line) == False:
                    if lr.isGeneLine(line):
                        currentGS.addGene(lr.getGeneSymbol(line))
                    line = f.readline()              

                yield( currentGS)
        except Exception, e:
            print "The following error occured at line: " + str(f.read(f.tell()).count('\n'))
            print e
        finally:
            f.close()


class LineReader(object):
    '''
    This class returns information about a line in PharmGKB pathway tsv file
    '''

    def __init__(self):
        pass

    def isDefLine(self, line):
        if( isinstance(line, basestring)):
            return True if (":" in line) and (line.count(":") == 1) else False
        else:
            raise TypeError("Expect input line to be string")

    def isGeneLine(self, line):
        if( isinstance(line, basestring)):
            return True if "Gene" in line[0:5] else False
        else:
            raise TypeError("Expect input line to be string")            

    def isDrugLine(self, line):
        if( isinstance(line, basestring)):
            return True if "Drug" in line[0:5] else False
        else:
            raise TypeError("Expect input line to be string")             

    def isDiseaseLine(self, line):
        if( isinstance(line, basestring)):
            return True if "Disease" in line else False
        else:
            raise TypeError("Expect input line to be string")                     

    def isConnectionHeader(self, line):
        if( isinstance(line, basestring)):
            return True if "From" in line[0:5] else False
        else:
            raise TypeError("Expect input line to be string") 

    def isConnectionLine(self, line):
        if( isinstance(line, basestring)):
            return True if (":" in line) and (line.count(":") > 1) else False
        else:
            raise TypeError("Expect input line to be string") 

    def isBlankLine(self, line):
        if( isinstance(line, basestring)):
            return (line.strip() == "")
        else:
            raise TypeError("Expect input line to be string") 

    def getGeneSymbol(self, line):
        if( self.isGeneLine(line)):
            tokens = line.split()
            if(len(tokens) == 3):
                return tokens[2].strip()
            else:
                raise ValueError("Current Gene line has invalid format")
        else:
            raise ValueError("The current line is not a Gene line")

    def getPathwayID(self, line):
        if(self.isDefLine(line)):
            tokens = line.split(":")
            return tokens[0].strip()
        else:
            raise ValueError("The current line is not a pathway definition line")

    def getPathwayDesc(self, line):
        if(self.isDefLine(line)):
            tokens = line.split(":")
            return tokens[1].strip()
        else:
            raise ValueError("The current line is not a pathway definition line")


    
def Main(argv):
    tsvp = TSVParser('pathways.tsv')
    gsetlist = []
    for gset in tsvp.getGeneSets():
        if(len(gset) > 0):
            gsetlist.append(gset)
        print "Parsed " + str(gset)
                                
    print "Total number of pathways with 1 or more genes: " + str(len(gsetlist))

    print "Generating output files"
    f1 = open('genesetmemb.txt', 'w')
    f2 = open('genesetname.txt', 'w')
    f3 = open('geneset.txt','w')
    for gset in gsetlist:
        f1.write(gset.toString()+"\n")
        f2.write(gset.getID() + ":" + gset.getDesc()+"\n")
        f3.write("NAME:\t" + gset.getID() + "\n")
        f3.write("DESC:\t" + gset.getDesc() + "\n")
        f3.write("MEMB:\t" + gset.toString()+"\n")
    f1.close()
    f2.close()
    f3.close()

    print "Output files generated"

if __name__ == '__main__':
    Main(sys.argv)

