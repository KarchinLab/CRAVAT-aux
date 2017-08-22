# This is a script containing common functions that you will have to use over and over again


# Maybe make this a class!!!

def remove_new_line_character(line):
    line = line.rstrip('\r\n')
    return line

# This function determines if the line read in the cravat output file is the start of variants section
# or if it is still the meta data at the top of the file
def determine_if_start_reading_cravat_output_file(line, file_type):
    start_reading = False
    if file_type == 'tsv':
        if line != '':
            if line[0] != '#':
                start_reading = True
    elif file_type == 'vcf':
        toks = line.split("\t")
        first_col =  toks[0]
        if first_col == "#CHROM":
            start_reading = True
    return start_reading

# This function takes the titles of a file and makes a dictionary with the keys
# being the titles and the values being parts of the line. The parts and titles are separated the same way in the line
def make_dictionary_of_titles_and_line_tabs(titles, toks):
    line_dict = {}
    tok_num = 0
    for tok in toks:
        line_dict[titles[tok_num]] = tok
        tok_num += 1
    return line_dict