#Gene Onotology Consortium 


#Need to know what format the annotation you are downloading is in
# get rid of NOT
# get rid of IEA
# load the aspect
# 
# http://geneontology.org/gene-associations/goa_human.gaf.gz
# http://purl.obolibrary.org/obo/go/go-basic.obo



#Build the database with the gene ontology consortium information

#Get
#      .gaf
#         -hugo
#         -go
#         -aspect
#      .obo
#         -go
#         -name
#         -aspect

# Build two database tables
#     BOTH OF THE TABLES SHOULDNT HAVE ASPECT. YOU WILL GET RID OF THAT AFTER TESTING
#     -go_id
#         -go
#         -name
#         -aspect P (biological process), F (molecular function) or C (cellular component)
#     -hugogo
#         -hugo
#         -go
#         -aspect

# You should only retrieve the go from the .obo file that you need
#     SO
#    first you will retrive all the distinct go symbols in the .gaf file

# The annotation_file contains the list of genes for the species and the GO Ids that are associated (the .gaf file)
# The go_file contains what the GO Ids stand for (the .obo file)
import os
import sys
import urllib2
import gzip
import MySQLdb
import traceback
db_connections = {
            "local":{
                "host": "localhost",
                "user": "root",
                "pwd" : "1234",
                "db_name": "cravat_annot_dev"
            },
            "docker":{
                "host": "localhost",
                "user": "root",
                "pwd" : "1",
                "db_name": "cravat_annot"
            }
        }
directory = os.path.join('/ext','sql')
concat_parse_unique_str = "$$$$$$$$$$$$$$"
go_data = {
        "annotation":{
                      "url": "http://geneontology.org/gene-associations/goa_human.gaf.gz",
                      "file_name": "goa_human.gaf",
                      "db_input_file_name":"go_annotation_data.txt",
                      "db_table": "go_annotation",
                      "db_table_schema": "hugo VARCHAR(100) NOT NULL DEFAULT '',"+\
                                    "go_id VARCHAR(100) NOT NULL DEFAULT ''," +\
                                    "go_aspect CHAR(1),"+\
                                    "PRIMARY KEY (hugo, go_id, go_aspect)",
              },
        "go":{
              "url": "http://purl.obolibrary.org/obo/go/go-basic.obo",
              "file_name": "go-basic.obo",
              "db_input_file_name": "go_name_data.txt",
              "db_table": "go_name",
              "db_table_schema": "go_id VARCHAR(100) NOT NULL DEFAULT '',"+\
                            "name VARCHAR(500) DEFAULT NULL,"+\
                            "PRIMARY KEY (go_id)",
              }
        }
#Annotation file column names and index
annotations_columns = {
                       'hugo': 2,
                       'qualifier': 3,
                       'go_id': 4,
                       'evidence': 6,
                       'aspect': 8
                       }
annotation_skipping = {
                     "qualifier":[
                                  "NOT"
                                  ],
                     "evidence":[
                                 "IEA",
                                 "ND"
                                 ]
                     }
# qualifier_vals_skipping = [
#                            "NOT"
#                            ]
# evidence_codes_skipping = [
#                            "IEA",
#                            "ND"
#                            ]


def get_data():
    get_annotation_file()
    get_go_file()
    return

def get_annotation_file():
    annotation_file_name = go_data["annotation"]["file_name"]
    annotation_gzfile_name = annotation_file_name + ".gz"
    annotation_gzfile_path = os.path.join(directory, annotation_gzfile_name)
    annotation_file_path = os.path.join(directory, annotation_file_name)
    download_file(go_data["annotation"]["url"], annotation_gzfile_path)
    unzip(annotation_gzfile_path, annotation_file_path)
    return

def download_file(url, file_path):
    print "downloading: " + str(file_path) + " from: " + str(url)
    response = urllib2.urlopen(url)
    content = response.read()
    downloaded_file = None
    try:
        downloaded_file = open(file_path, 'wb')
        downloaded_file.write(content)
        print "finished downloading"
    finally:
        try:
            downloaded_file.close()
        except Exception, q:
            pass
    return

def unzip(gz_file, unzipped_file):
    print "unzipping: " + gz_file + " to: " + unzipped_file
    infile = None
    outfile = None
    try:
        with gzip.open(gz_file, 'rb') as infile:
            with open(unzipped_file, 'w') as outfile:
                for line in infile:
                    outfile.write(line)
        print "finished unzipping"
    finally:
        try:
            infile.close()
        except:
            pass
        try:
            outfile.close()
        except:
            pass
    return

def get_go_file():
    go_file_path = os.path.join(directory, go_data["go"]["file_name"])
    download_file(go_data["go"]["url"], go_file_path)
    return

# Parse the annotation file retrieving the following columns
#     -hugo
#     -go id
#     -aspect
# Skip the following
#     -Qualifier:
#         - NOT
#     -Evidence:
#         - IEA, ND
def build_annotation_input():
    # Pick out the hugo, go, aspect combos
    rf = None
    wf = None
    hugo_go_aspects_seen = {}
    gos = {}
    try:
        rf = open(os.path.join(directory, go_data["annotation"]["file_name"]), 'r')
        wf = open(os.path.join(directory, go_data["annotation"]["db_input_file_name"]), 'w')
        for line in rf:
            line = remove_new_line_character(line)
            if line[0] == '!':
                continue
            toks = line.split('\t')
            hugo  = toks[annotations_columns['hugo']]
            qualifier = toks[annotations_columns['qualifier']]
            go_id = toks[annotations_columns['go_id']]
            evidence_code = toks[annotations_columns['evidence']]
            aspect = toks[annotations_columns['aspect']]
            if qualifier in annotation_skipping["qualifier"] or evidence_code in annotation_skipping["evidence"]:
                continue
            # Collect the unique go_ids in the annotation file. You are using a dictionary. It will end up being unique
            gos[go_id] = True
            # Collecting unique hugo, go, aspect. There is more evidence if it occurs more than once I guess but we are not currently recording that
            hugo_go_aspect = str(hugo) + concat_parse_unique_str + str(go_id) + concat_parse_unique_str + str(aspect)
            if hugo_go_aspect not in hugo_go_aspects_seen:
                wf.write(hugo + "\t" + go_id + "\t" + aspect + "\n")
                hugo_go_aspects_seen[hugo_go_aspect] = True
    finally:
        try:
            rf.close()
        except:
            pass
        try:
            wf.close()
        except:
            pass
    return gos

# Build a database input file of the hugo, go aspects





# I DON"T LIKE HOW I AM USING $$$$$$$$$$$$$$. THINK OF SOMETHING BETTER

def build_go_input(gos_retrieving):
    print 'parse_go'
    go_initiator = '[Term]'
    
    rf = None
    try:
        rf = open(os.path.join(directory, go_data["go"]["file_name"]), 'r')
        go = None
        look_for_id_and_name = False
        for line in rf:
            line = remove_new_line_character(line)
            if look_for_id_and_name:
                if line[:6] == 'id: GO':
                    if go != None:
                        raise "GO was not None when it should have been"
                    go = line.split(' ')[1]
                elif line[:5] == 'name:':
                    if go in gos_retrieving:
                        if gos_retrieving[go] != True:
                            print "The go "+go+" has the name "+gos_retrieving[go]+" and "+line.split(':')[1][1:]
                        gos_retrieving[go] = line.split(':')[1][1:]
                    go = None
                    look_for_id_and_name = False
            else:
                if line == go_initiator:
                    look_for_id_and_name = True
                continue
    finally:
        try:
            rf.close()
        except:
            pass
    ordered_gos = gos_retrieving.keys()
    ordered_gos.sort()
    wf = None
    try:
        wf = open(os.path.join(directory, go_data["go"]["db_input_file_name"]), 'w')
        for o_go in ordered_gos:
            wf.write(o_go + "\t" + str(gos_retrieving[o_go]) + "\n")
    finally:
        try:
            wf.close()
        except:
            pass
    
    return

def build_and_load_db_tables(server):
    print 'building database tables'
    db_name = "cravat_annot"
    db_conn = None
    cursor = None
    try:
        db_conn = MySQLdb.connect(host=db_connections[server]['host'],
                 user=db_connections[server]['user'],
                 passwd=db_connections[server]['pwd'],
                 db=db_connections[server]['db_name'])
        cursor = db_conn.cursor()
        #Build the database tables
        for db_type in go_data.keys():
            db_table = go_data[db_type]["db_table"]
            table_schema = go_data[db_type]["db_table_schema"]
            cursor.execute("drop table if exists "+db_table)
            #Create database table
            print '\tbuilding ' + str(db_table)
            sql_create = "create table "+db_table+" ("+table_schema+") ENGINE=InnoDB DEFAULT CHARSET=latin1"
            cursor.execute(sql_create)
            #Load the database table
            print '\tloading ' + str(db_table) + ' with ' + os.path.join(directory, go_data[db_type]["db_input_file_name"])
            sql_load = "load data infile '"+os.path.join(directory, go_data[db_type]["db_input_file_name"])+"' into table "+db_table
#             print '\t\t' + sql_load
            cursor.execute(sql_load)
        db_conn.commit()
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            db_conn.close()
        except:
            pass
        
    return

def remove_new_line_character(line):
    line = line.rstrip('\r\n')
    return line

if __name__ == "__main__":
    try:
        server = sys.argv[1]
        get_data()
        # Build the annotation input and retrieve the go_ids that are needed
        gos_in_annotation = build_annotation_input()
        build_go_input(gos_in_annotation)
        build_and_load_db_tables(server)
    except Exception, e:
        sys.stderr.write(str(traceback.format_exc()))
