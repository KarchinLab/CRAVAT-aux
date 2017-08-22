import MySQLdb

db = MySQLdb.connect(host='localhost', user='andywong86', passwd='andy_wong_mysql+86', db='cravat_results')
cursor = db.cursor()

f = open('Interactive_result_header_to_column_table_data.txt')
columns = []
column_defs = {}
for line in f:
    [level, group, header, col, definition, hidden, order] = line[:-1].split('\t')
    