def switch_strand(base):
    if base == 'A':
        match_base = 'T'
    elif base == 'T':
        match_base = 'T'
    elif base == 'G':
        match_base = 'C'
    elif base == 'C':
        match_base = 'G'
    return match_base


def cravat_to_vcf(cravat_path,vcf_path):
    cravat_file = open(cravat_path,'r')
    cravat_lines = cravat_file.read().split('\n')
    vcf_head = '##fileformat=VCFv4.1\n#CHROM    POS    ID    REF    ALT    QUAL    FILTER    INFO\n'
    vcf_lines = []
    for line in cravat_lines:
        if line.startswith('#'):
            continue
        
        cravat_cols = line.split('\t')
        vcf_cols = []
        vcf_cols.append(cravat_cols[1])
        vcf_cols.append(cravat_cols[2])
        vcf_cols.append(cravat_cols[3])
        if cravat_cols[3] == '-':
            vcf_cols.append(switch_strand(cravat_cols[4]))
        else:
            vcf_cols.append(cravat_cols[4])
        vcf_cols.append(cravat_cols[5])
        vcf_cols.append('25')
        vcf_cols.append('PASS')
        vcf_cols.append('BLANK')
        
        vcf_lines.append('\t'.join(vcf_cols))
    vcf_text = vcf_head + '\n'.join(vcf_lines)
    vcf_file = open(vcf_path,'w')
    vcf_file.write(vcf_text)
    vcf_file.close()
    cravat_file.close()
    
if __name__ == "__main__":
    c_path = 'C:/Users/Kyle/cravat/testing/test_cases/#test_conversion/cravat.txt'
    v_path = 'C:/Users/Kyle/cravat/testing/test_cases/#test_conversion/vcf.txt'
    cravat_to_vcf(c_path,v_path)
    