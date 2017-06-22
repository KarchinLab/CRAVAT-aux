import os
import sys
import shutil

if __name__ == '__main__':
    test_cases_dir = os.path.abspath(sys.argv[1])
    case_dirs = []
    for item_name in os.listdir(test_cases_dir):
        item_path = os.path.join(test_cases_dir, item_name)
        if os.path.isdir(item_path):
            case_dirs.append(item_path)
    for case_dir in case_dirs:
        case_name = os.path.basename(case_dir)
        print case_dir
        input_dirs = [os.path.join(case_dir, x) for x in os.listdir(case_dir)]
        get_type = lambda x: x.split('_')[-1]
        itypes = map(get_type, input_dirs)
        c_present = 'c' in itypes
        if c_present:
            for input_dir in input_dirs:
                itype = get_type(input_dir)
                if itype == 'c':
                    file_paths = [os.path.join(input_dir, x) for x in os.listdir(input_dir)]
                    for file_path in file_paths:
                        fname = os.path.basename(file_path)
                        fname_new = '_'.join([case_name, fname.split('_')[-1]])
                        file_path_new = os.path.join(case_dir, fname_new)
                        print file_path, file_path_new
                        shutil.copy(file_path, file_path_new)
                    shutil.rmtree(input_dir)
                else:
                    print input_dir, 'REMOVE'
                    shutil.rmtree(input_dir)
        else:
            print '\tMANUAL'