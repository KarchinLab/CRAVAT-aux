import os
import json
import sqlite3

conn = sqlite3.connect('ndex.sqlite')
cursor = conn.cursor()

table = 'hugo2network'
cursor.execute('drop table if exists ' + table)
cursor.execute('create table ' + table + ' (hugo string, ' +\
    'networkid string)')
cursor.execute('drop index if exists ' + table + '_idx0')
cursor.execute('create index ' + table + '_idx0 on ' + table + ' (hugo)')
table = 'network'
cursor.execute('drop table if exists ' + table)
cursor.execute('create table ' + table + ' (networkid string primary key, ' +\
    'networkname string)')

esetdir = '/ext/resource/e_sets/cravat_nci'
ndexfiles = [v for v in os.listdir(esetdir) if v[-5:] == '.json']
wf = open('ndex.txt', 'w')
networkidname = {}
for ndexfile in ndexfiles:
    path = os.path.join(esetdir, ndexfile)
    f = open(path)
    data = json.loads(f.readline())
    f.close()
    networkid = data['network_id']
    networkname = data['name']
    hugos = data['ids']
    for hugo in hugos:
        wf.write('\t'.join([hugo, networkid]) + '\n')
    networkidname[networkid] = networkname
wf.close()

wf = open('ndexnetwork.txt', 'w')
for networkid in networkidname.keys():
    wf.write('\t'.join([networkid, networkidname[networkid]]) + '\n')
wf.close()
