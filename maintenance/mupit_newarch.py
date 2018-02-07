import sqlite3
import os

oldconn = sqlite3.connect('d:\\dockervolume\\sql\\mupit.sqlite')
oc = oldconn.cursor()
newconn = sqlite3.connect('d:\\dockervolume\\temp\\mupit.sqlite')
nc = newconn.cursor()

newtable = 'mupit'
nc.execute('drop table if exists ' + newtable)
nc.execute('create table ' + newtable +\
                  ' (chrom string, start int, stop int)')
nc.execute('create index ' + newtable +\
                  '_idx0 on ' + newtable + '(chrom, start)')

oc.execute('select distinct chr from Genome2PDB')
chroms = [v[0] for v in oc.fetchall()]
print(chroms)
exit()
oc.execute('select chr, pos1, pos2, pos3 from Genome2PDB')