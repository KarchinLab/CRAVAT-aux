import cPickle
import sys

def makeRefseq2Gene():
    f=open('d:\\chasm\\gene2refseq')
    refseq2gene = {}
    count=0
    for line in f:
        count += 1
        if count % 100000 == 0:
            print count
        if line[0] != '#':
            toks = line.split('\t')
            speciesNo = toks[0]
            if speciesNo == '9606':
                geneId = toks[1]
                rnaAcc = toks[3].split('.')[0]
                proteinAcc = toks[5].split('.')[0]
                genomicAcc = toks[7].split('.')[0]
                refseq2gene[rnaAcc] = geneId
                refseq2gene[proteinAcc] = geneId
                refseq2gene[genomicAcc] = geneId
    f.close()
    wf=open('d:\\chasm\\chasmweb\\chasmweb\\webcontent\\wrappers\\refseq2gene-human','wb')
    cPickle.dump(refseq2gene, wf, -1)
    wf.close()

def makeGene2Symbol():
    f=open('d:\\nci60\\gene\\gene_info_human')
    gene2Symbol = {}
    for line in f:
        toks = line.strip().split('\t')
        geneId = toks[1]
        symbol = toks[2]
        gene2Symbol[geneId] = symbol
    f.close()
    wf=open('d:\chasm\\chasmweb\\chasmweb\\webcontent\\wrappers\\gene2symbol','wb')
    cPickle.dump(gene2Symbol,wf,-1)
    wf.close()

def makeEnsemble2Gene():
    f=open('d:\\cravat\\gene2ensembl')
    ensembl2gene = {}
    for line in f:
        if line[0] != '#':
            toks = line.split('\t')
            speciesNo = toks[0]
            if speciesNo == '9606':
                gene_id = toks[1]
                ensembl_gene_acc = toks[2]
                ensembl_rna_acc = toks[4]
                ensembl_protein_acc = toks[6]
                ensembl2gene[ensembl_gene_acc] = gene_id
                ensembl2gene[ensembl_rna_acc] = gene_id
                ensembl2gene[ensembl_protein_acc] = gene_id
    f.close()
    wf = open('d:\\cravat\\cravat\\webcontent\\wrappers\\ensembl_to_gene_id_map_human','wb')
    cPickle.dump(ensembl2gene, wf, -1)
    wf.close()

#makeGene2Symbol()
makeEnsemble2Gene()