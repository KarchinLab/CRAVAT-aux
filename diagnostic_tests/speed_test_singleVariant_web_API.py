
# This file will test the speed of the single variant we api
import os
import sys
import requests
import time

def pick_job_and_initiate(dir_with_jobs):
    
    jobs = os.listdir(dir_with_jobs)
#     This will go through a directory that has many different files that will be used as input
    for job in jobs:
        if job == ".DS_Store":
            continue
        variants = make_array_of_all_input(os.path.join(dir_with_jobs, job))
        total_num_variants = len(variants)
        totals, array_randoms_for_totals = decided_on_number_of_variants(total_num_variants)
        call_web_api(variants, totals, array_randoms_for_totals)
    
    return


def make_array_of_all_input(job_file):
    
    variants = []
    
    try:
        rf = open(job_file, 'rU')
        for line in rf:
            line = line.rstrip("\n\r")
            variants.append(line)
            
        rf.close()
    except Exception, e:
        try:
            rf.close()
        except Exception, f:
            print "inside second try in make_array_of_all_input"
        print e
    
    return variants



def call_web_api(variants, totals, array_randoms_for_totals):

    for i in range(0, len(totals)):
        num_var = totals[i]
        array_randoms = array_randoms_for_totals[i]

        time_before = time.time()

        for random in array_randoms:
            variant = variants[random]
            variant_toks = variant.split("\t")
            uid = variant_toks[0]
            chrom = variant_toks[1]
            pos = variant_toks[2]
            strand = variant_toks[3]
            ref_base = variant_toks[4]
            alt_base = variant_toks[5]
    #     staging webAPI
            url = "http://staging.cravat.us/rest/service/query?mutation="+chrom+"_"+pos+"_"+strand+"_"+ref_base+"_"+alt_base
            
            r = requests.get(url)
        time_after = time.time()
        
        time_took = time_after - time_before
        print "For " +str(num_var)+ " variants, the WebAPI took "+str(time_took)+" seconds"
    
    return


def decided_on_number_of_variants(total_num_variants):
    
    totals = [1, 10, 100, 1000, 10000, 100000]
    array_randoms = []   
         
    try:
        totals = [1, 10, 100, 1000, 10000, 100000]
        array_randoms = []
        
        for total in totals:
            array_randoms_this_total = []
            random_num_on = 0
            for x in range(0, total):
                if total == 1:
                    random_num_on += 53
                elif total == 10:
                    random_num_on += 31
                elif total == 100:
                    random_num_on += 19
                elif total == 1000:
                    random_num_on += 17
                elif total == 10000:
                    random_num_on += 13
                elif total == 100000:
                    random_num_on += 1
                if random_num_on > total_num_variants:
                    sys.stderr.write("The random number "+str(random_num_on)+ " is greater than the number of variants. total = "+str(total)+" This cannot be used. Fix this!")
                    sys.exit(-1)
                array_randoms_this_total.append(random_num_on)
            array_randoms.append(array_randoms_this_total)
    
    except Exception, f:
        sys.stderr.write(str(f))
        sys.exit(-1)
    
    return totals, array_randoms


if __name__ == "__main__":
    
    input_path = os.path.join("/Users", "derekgygax", "Desktop", "CRAVAT_Info_Test", "API", "how_fast")
    pick_job_and_initiate(input_path)