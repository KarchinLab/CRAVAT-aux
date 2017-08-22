
import sys



# program that tests if the CRAVAT output from the webpages are the same as the output from the API


def read_standard(file_name):
    
    
    
    file_path = "/Users/derekgygax/Desktop/CRAVAT_Info_Test/API/testing/"
    
    
    rf = open(file_path+file_name, "rU")
    
    standard_results = {}
    column_titles = []
    
    for line in rf:
        line = line.rstrip("\n\r")
        toks = line.split("\t")
        
        id_for_variant = toks[0]
        
        if (toks[0] == "ID" and toks[1]=="Chromosome" and toks[2]=="Position"):
            for tok in toks:
                column_titles.append(tok)
        else:
#             if standard_results.has_key(id_for_variant):
            if id_for_variant in standard_results:
                sys.stderr.write("THIS ID "+id_for_variant+" OCCURRED MORE THAN ONCE. THAT IS NOT OK!!!")
                sys.exit(-1)
            else:
                variant_info = {}
                
                for column_num in range(0,len(column_titles)):
                    title = column_titles[column_num]
                    variant_info[title] = toks[column_num]
            
                standard_results[id_for_variant] = variant_info
                  
    read_API(standard_results)
    
    return



def read_API(standard_results):
    
    import requests
    
    api_results = {}
    
    for variant in standard_results:
        
        variant_info = standard_results[variant]
        

        
        uid = variant_info["ID"]
        chrom = variant_info["Chromosome"]
        pos = variant_info["Position"]
        strand = variant_info["Strand"]
        refBase = variant_info["Reference base"]
        altBase = variant_info["Alternate base"]
     
        url = "http://staging.cravat.us/rest/service/query?mutation="+chrom+"_"+pos+"_"+strand+"_"+refBase+"_"+altBase
        
        
        r = requests.get(url)
        
        api_variant = r.json()
        
        
        if variant in api_results:
            sys.stderr.write("The ID "+variant+" is in the variant list from the standard results twice! WHAT IS THAT!!!")
            sys.exit(-1)
        else:
            api_results[variant] = api_variant       
        
       
    compare_files(standard_results, api_results)
        
    return



def compare_files(standard_results, api_results):
    
    print "standard to api"
    num_variants = 0
    for variant in standard_results:
        num_variants += 1
        if num_variants > 1:
            print "\n"
            
        print "\t" + str(variant)
        if variant not in api_results:
            "\t\tVariant in Standard but not in API!!"
        else:
            standard_variant = standard_results[variant]
            api_variant = api_results[variant]
            
            for column in standard_variant:
                
                if column not in api_variant:
                    print "\t\t" + str(column)
                    print "\t\t\tColumn in Standard but not in API!!!!"
                else:
                    standard_value = standard_variant[column]
                    api_value = api_variant[column]
                    compare_values(column, standard_value, api_value)
                    

                        
    print "\n\n\n"
    print "api to standard"
    num_var_2 = 0
    for variant in api_results:
        num_var_2 += 1
        if num_var_2 > 1:
            print "\n"
        print "\t" + "Variant ID: " + str(variant)
        if variant not in standard_results:
            "\t\tVariant in API but not in Standard!!"
        else:
            standard_variant = standard_results[variant]
            api_variant = api_results[variant]
            
            for column in api_variant:
                
                if column not in standard_variant:
                    print "\t\t" + str(column)
                    print "\t\t\tColumn in API but not in Standard!!!!"
                else:
                    standard_value = standard_variant[column]
                    api_value = api_variant[column]
                    
                    compare_values(column, standard_value, api_value)
                     
#                     if standard_value != api_value:
#                         print "\t\t" + str(column)
#                         print "\t\t\t" + "standard = " + str(standard_value)
#                         print "\t\t\t" + "api = " + str(api_value)
     
    return




def compare_values(title, std_value, api_value):
    
    import re
    regEx_freq = re.compile("frequency")
    
    if regEx_freq.search(title):
        std_value = "%.5f" % float(std_value)      #This converts the value to a rounded decimal
        api_value = "%.5f" % float(api_value)      #This converts the value to a rounded decimal
    
    if std_value != api_value:
        print "\t\t" + str(title)
        print "\t\t\tapi = " + str(api_value)
        print "\t\t\tstandard = " + str(std_value)
    
    return 




if __name__ == "__main__":
    
    file_name = sys.argv[1]
    
    read_standard(file_name)
    
    
    
    
    
    
    

