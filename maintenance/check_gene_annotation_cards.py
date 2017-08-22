import sys
import os
import MySQLdb
import re

def main():
    
    #connect to database
    mysql_host = 'karchin-db01.icm.jhu.edu'
    mysql_user = 'mryan'
    mysql_password = 'royalenfield'
    db_name = 'SNVBox_dev'
    
    try:
#     Open database connection
        db = MySQLdb.connect(host=mysql_host,\
                    user=mysql_user,\
                    passwd=mysql_password,\
                    db=db_name)
    
        cursor_db = db.cursor()
    
        dir_gene_annotation_cards = '/home/dgygax/geneAnnotCard/geneCardAnnotCache/'
        dir_output_files = '/home/dgygax/GeneSymbolsAndAnnotationCard/'
    
#     Retrieve all the gene symbols from the database using the function 'retrieve_list_gene_symbols()'
        list_of_gene_symbols = retrieve_list_gene_symbols(db, cursor_db)
    
#     Close database connection
        cursor_db.close()
        db.close()
        
#         Making both of these variables equal to None after the database connection has been closed
        cursor_db = None
        db = None
    
#     Get the number of gene symbols in the database
        num_gene_symbols_in_db = len(list_of_gene_symbols)
    
#     Check which gene symbols do not have a gene annotation card and which gene symbols have a blank gene annotation card
        gene_symbols_no_annotation_card, gene_symbols_blank_annotation_card, num_gene_symbol_in_DB_but_not_cache = examine_if_gene_symbols_have_gene_annotation_cards(dir_gene_annotation_cards, list_of_gene_symbols, num_gene_symbols_in_db)
    
#     Write out the gene symbols that don't have an annotation card or the ones that have a blank card
        output_no_card = open(dir_output_files + 'no_gene_annotation_Card.txt', 'w')
        output_blank_card = open(dir_output_files + 'blank_gene_annotation_Card.txt', 'w')
    
        for gene_no_card in gene_symbols_no_annotation_card:
            output_no_card.write(gene_no_card + '\n')

        for gene_blank_card in gene_symbols_blank_annotation_card:
            output_blank_card.write(gene_blank_card + '\n')
            
        print 'Number of gene symbols in database that are not in the gene cache = ' + str(num_gene_symbol_in_DB_but_not_cache)
#     
#     print 'gene_symbols_no_annotation_card = ' + str(gene_symbols_no_annotation_card)
#     print 'gene_symbols_blank_annotation_card = ' + str(gene_symbols_blank_annotation_card)


        output_no_card.close()
        output_blank_card.close()

    except MySQLdb.Error, databaseProblem:
        sys.stderr.write("The following problem occurred with the database:" + "\n\n" + "%s" %databaseProblem + "\n\n\n")
        try:
            cursor_db.close()
            db.close()
        except NameError:
            standAlonePlaceHold = 5
    except Exception, f:
        sys.stderr.write("The following problem has occured %s" %f)
        if cursor_db != None:
            cursor_db.close()
        if db != None:
            db.close()
        sys.exit(-1)    
        
    print 'made it to the end!!!'
        
    return


# This function goes in the database and retrieves all the gene symbols in the table 'GeneSymbols' 
# that have a UID matching a UID in the table 'CodonTable' 
def retrieve_list_gene_symbols(db, cursor_db):
    
    list_of_gene_symbols = []
    
    
    try:
        
        sql = 'select distinct(g.GeneSymbol) from GeneSymbols g, CodonTable c where g.UID = c.UID;'
        
        cursor_db.execute(sql)
        all_gene_symbols = cursor_db.fetchall()
        
        for each_symbol in all_gene_symbols:
            list_of_gene_symbols.append(each_symbol[0])
        
    except MySQLdb.Error, catch_database_problem:
        sys.stderr.write("The following problem occurred with the database:" + "\n\n" + "%s" %catch_database_problem + "\n\n\n")
        print "The following problem occurred with the database:" + "\n\n" + "%s" %catch_database_problem + "\n\n\n"
        try:
            cursor_db.close()
            db.close()
        except NameError:
            standAlonePlaceHold = 5
        
 
        sys.exit(-1)
    
    except Exception, e:
        cursor_db.close()
        db.close()
        sys.stderr.write("The following problem has occured %s" %e)
        print "The following problem has occured %s" %e
        sys.exit(-1)
        
    
    return list_of_gene_symbols



# This function checks every gene symbol in the input parameter 'list_of_gene_symbols' to see if the gene symbol has  
# a corresponding gene annotation card
def examine_if_gene_symbols_have_gene_annotation_cards(dir_gene_annotation_cards, list_of_gene_symbols, num_gene_symbols_in_db):
    
    dict_gene_annotation_card_symbols = {}
    dict_gene_annotaiton_cards = {}
    
#     gene_symbols_that_don't have a corresponding gene annotation card
    gene_symbols_no_annotation_card = []
    
#     gene_symbols_that have a gene annotation card but the annotation card is blank
    gene_symbols_blank_annotation_card = []
    
    list_gene_annotation_cards =  os.listdir(dir_gene_annotation_cards)
    
#     Get a dictionary containing the gene symbols for all the gene annotations cards
#     Also making a dictionary where the keys are the gene symbols and the values are the name of the card
    for gene_card in list_gene_annotation_cards:
        gene_symbol_for_card =  gene_card[:gene_card.index('.')]
        dict_gene_annotaiton_cards[gene_symbol_for_card] = gene_card
        dict_gene_annotation_card_symbols[gene_symbol_for_card] = True
    
    
#     This variable gathers the number of the gene symbols in the database that not in the gene annotation cache
    num_gene_symbol_in_DB_but_not_cache = 0
    
#     Go through the 'list_of_gene_symbols' and see which gene_symbols have a gene annotation card
    for gene_symbol_from_DB in list_of_gene_symbols:
#         if the symbol is in the dictionary of gene annotation card go to a function to check the contents of the gene annotation card
        if gene_symbol_from_DB in dict_gene_annotation_card_symbols:
            gene_annotation_card_has_something = check_contents_of_anotation_card(dir_gene_annotation_cards, dict_gene_annotaiton_cards[gene_symbol_from_DB])
            
#             If the gene annotation card is blank then put it in the list 'gene_symbols_blank_annotation_card'
            if gene_annotation_card_has_something == 0:
                gene_symbols_blank_annotation_card.append(gene_symbol_from_DB)
            
#         If the gene symbol does not contains an annotation card then put it in the list 'gene_symbols_no_annotation_card'
        else:
            gene_symbols_no_annotation_card.append(gene_symbol_from_DB)
            num_gene_symbol_in_DB_but_not_cache += 1
            
        
#     Return the lists of gene symbols that either don't have an annotation card or
#     have an annotations card that is blank 
    return gene_symbols_no_annotation_card, gene_symbols_blank_annotation_card, num_gene_symbol_in_DB_but_not_cache


# This function checks if the gene annotation card has an alphanumeric character in any of the lines
def check_contents_of_anotation_card(dir_gene_annotation_cards, gene_card_examining):
    
#     regular expression for alphanumeric character
    word_or_number_character = re.compile('\w')
    
#     this is a variable to signify if the card has something or not. 0 means it doesn't. One means it does
    card_has_something = 0
    
#     The file being examined
    gene_card = open(dir_gene_annotation_cards+gene_card_examining, 'r')
    
    for the_line in gene_card:
        line_has_word_or_number = word_or_number_character.search(the_line)
        
#         This is checking if a line in the card has a word or a number. If it does then change 'card_has_something' to 1 and exit the loop
        if line_has_word_or_number:
            card_has_something = 1
            break
    
    gene_card.close()
    return card_has_something



if __name__ == '__main__':
    main()
    
    