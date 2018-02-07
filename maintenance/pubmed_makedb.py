import sqlite3
import os

conn = sqlite3.connect('d:\\dockervolume\\resource\\pubmed.sqlite')
cursor = conn.cursor()
cursor.execute('drop table if exists pubmed')
cursor.execute('create table pubmed ' +\
    '(hugo string primary key, n int, term string)')

folder = 'd:\\dockervolume\\resource\\pubmedSearchResultCache'
files = os.listdir(folder)
data = {}
count = 0
for file in files:
    count += 1
    if count % 100 == 0:
        print(count)
    hugo = file.split('.')[0]
    path = os.path.join(folder, file)
    f = open(path)
    term = (f.readline()).strip()
    n = (f.readline()).strip()
    data[hugo] = {'n': n, 'term': term}
    f.close()
hugos = list(data.keys())

for hugo in hugos:
    cursor.execute('insert into pubmed values ' +\
        '("' + hugo + '", ' + data[hugo]['n'] + ', "' +\
        data[hugo]['term'] + '")')

conn.commit()