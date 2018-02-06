import os
import shutil
import sys
import XMLConverter
import yaml

test_case_dir = os.path.abspath(sys.argv[1])
case_dirs = [os.path.join(test_case_dir, x) for x in os.listdir(test_case_dir)]
for case_dir in case_dirs:
    if not(os.path.isdir(case_dir)): continue
    print(case_dir)
    for fname in os.listdir(case_dir):
        if fname != 'desc.xml': continue
        print(fname)
        fpath = os.path.join(case_dir, fname)
        d = XMLConverter.xml_to_dict(fpath)
        t = yaml.dump(d, default_flow_style=False)
        fname_new = fname.replace('.xml','.yml')
        fpath_new = os.path.join(case_dir, fname_new)
        with open(fpath_new, 'w') as wf:
            wf.write(t)
        os.remove(fpath)