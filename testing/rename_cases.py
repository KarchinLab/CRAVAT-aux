import os
import shutil
import sys

test_case_dir = os.path.abspath(sys.argv[1])
case_dirs = [os.path.join(test_case_dir, x) for x in os.listdir(test_case_dir)]
for case_dir in case_dirs:
    if not(os.path.isdir(case_dir)): continue
    print(case_dir)
    for fname in os.listdir(case_dir):
        print(fname)
        fname_new = ''
        fpath_old = os.path.join(case_dir, fname)
        if fname.endswith('input.txt'):
            fname_new = 'input.txt'
        elif fname.endswith('key.tsv'):
            fname_new = 'key.tsv'
        elif fname.endswith('desc.xml'):
            fname_new = 'desc.xml'
        else:
            raise Exception(fpath_old)
        fpath_new = os.path.join(case_dir, fname_new)
        shutil.move(fpath_old, fpath_new)