import csv
path = 'C:/Users/Kyle/cravat/testing/test.csv'
with open(path) as r:
        a = csv.DictReader(r)
        d = {}
        for row in a:
            d[row['uid']] = row
for row in d: print row, d[row]