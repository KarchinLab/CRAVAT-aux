# import MySQLdb
# db=MySQLdb.connect(host='karchin-db01.icm.jhu.edu',db='SNVBox_dev',user='andywong86',passwd='andy_wong_mysql+86')
# cursor=self.db.cursor()
# 
# cursor.execute('select UID, max(Pos) as mp from CodonTable group by UID')
# results = cursor.fetchall()
# wf = open('codontable_uid_maxpos.txt', 'w')
# for result in results:
#     (uid, maxpos) = result
#     wf.write(uid + '\t' + str(maxpos) + '\n')
# wf.close()
# 
# cursor.execute('select UID, max(Pos) as mp from Regional_Comp group by UID')
# results = cursor.fetchall()
# wf = open('regionalcomp_uid_maxpos.txt', 'w')
# for result in results:
#     (uid, maxpos) = result
#     wf.write(uid + '\t' + str(maxpos) + '\n')
# wf.close()

uidmaxpos_codontable = {}
f = open('c:\\projects\\cravat\\error jobs\\codontable_maxpos.txt')
for line in f:
    [uid, maxpos] = line[:-1].split('\t')
    uidmaxpos_codontable[int(uid)] = int(maxpos)
f.close()

uidmaxpos_regionalcomp = {}
f = open('c:\\projects\\cravat\\error jobs\\regionalcomp_maxpos.txt')
for line in f:
    [uid, maxpos] = line[:-1].split('\t')
    uidmaxpos_regionalcomp[int(uid)] = int(maxpos)
f.close()

uids = uidmaxpos_codontable.keys()
uids.sort()

difcount = 0
for uid in uids:
    if uidmaxpos_regionalcomp.has_key(uid):
        maxpos_codontable = uidmaxpos_codontable[uid]
        maxpos_regionalcomp = uidmaxpos_regionalcomp[uid]
        #if maxpos_codontable != maxpos_regionalcomp + 1 and maxpos_codontable != maxpos_regionalcomp:
        if abs(maxpos_codontable - maxpos_regionalcomp) >= 4:
            difcount += 1
            print '%4d\t%8s\t%5d\t%5d'%(difcount, uid, maxpos_codontable, maxpos_regionalcomp)
    else:
        print uid, 'not in Regional_comp'

print len(uids), 'UIDs in CodonTable'