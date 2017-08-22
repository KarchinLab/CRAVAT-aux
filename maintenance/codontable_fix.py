import MySQLdb


print 'getting db'
db=MySQLdb.connect(host='karchin-db01.icm.jhu.edu',db='SNVBox_dev',user='andywong86',passwd='andy_wong_mysql+86')
print 'getting cursor'
cursor=db.cursor()

print 'executing select'
cursor.execute('select * from CodonTable limit 1000000')

print 'fetching results'
results = cursor.fetchall()
print len(results), 'results'