import os

path = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\hg18'
add = '18'
os.chdir(path)
dirs = os.listdir(os.getcwd())
print dirs
for d in dirs:
    os.chdir(d)
    print os.getcwd()
    for item in os.listdir(os.getcwd()):
        spl = item.split('_')
        start = '_'.join(spl[:-1])
        end = spl[-1]
        new_name = '_'.join([start,add,end])
        os.rename(item,new_name)
    print os.listdir(os.getcwd())
    os.chdir(os.path.pardir)
    os.rename(d,d + '_' + add)