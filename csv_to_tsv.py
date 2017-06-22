import os
import shutil
import sys

def csv_to_tsv(csv_path):
    basedir, fname = os.path.split(csv_path)
    temp_path = os.path.join(basedir,fname+'.temp')
    shutil.copy(csv_path, temp_path)
    with open(temp_path,'rU') as f, open(csv_path,'w') as wf:
        for l in f:
            wf.write(l.replace(',','\t'))
    tsv_path = '.'.join(csv_path.split('.')[:-1]+['tsv'])
    shutil.move(csv_path, tsv_path)
    os.remove(temp_path)
    
if __name__ == '__main__':
    test_cases_dir = os.path.abspath(sys.argv[1])
    case_dirs = []
    for item_name in os.listdir(test_cases_dir):
        item_path = os.path.join(test_cases_dir, item_name)
        if os.path.isdir(item_path):
            case_dirs.append(item_path)
    for case_dir in case_dirs:
        fpaths = [os.path.join(case_dir, x) for x in os.listdir(case_dir)]
        get_type = lambda x: x.split('_')[-1]
        for fpath in fpaths:
            if fpath.split('.')[-1] == 'csv':
                csv_to_tsv(fpath)