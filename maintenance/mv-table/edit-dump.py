import os
import sys

def loop_sql(path):
    f = open(path,'r')
    t = f.read()
    buffer = ''
    for c in t:
        buffer += c
        if c == ';':
            yield buffer
            buffer = ''
    

if __name__ == '__main__':
    base_path = os.path.abspath(sys.argv[1])
    wf = open(base_path.replace('.sql','.datadir.sql'),'w')
    data_path = '/ext/temp/mysql/cravat_results'
    for l in loop_sql(base_path):
        if l.lstrip().lower().startswith('create table'):
            l = l.replace(';',' DATA DIRECTORY = \'%s\';\n' %data_path)
        wf.write(l)
        
    wf.close()