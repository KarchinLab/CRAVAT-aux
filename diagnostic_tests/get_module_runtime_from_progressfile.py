import sys

progress_filename = sys.argv[1]

f = open(progress_filename)
module_runtimes = {}
current_module = ''
for line in f:
    if line[0] == '#':
        continue
    toks = line.strip().split('\t')
    if toks[0] == '>':
        [dummy, time, module] = toks
        
        if current_module != '':
            if current_module == 'VEST_MS' or 'VEST' not in current_module or ('VEST' in current_module and repeat_no == '9'):
                module_runtimes[current_module] = int(float(time) - module_runtimes[current_module])
        
        if module == 'VEST':
            module = 'VEST_MS'
            module_runtimes[module] = float(time)
            current_module = module
        elif module == 'VEST IV':
            module = 'VEST_IV'
            module_runtimes[module] = float(time)
            current_module = module
        elif 'VEST' in module:
            [dummy, so, repeat_no, dummy, dummy] = module.split(' ')
            module = 'VEST_' + so
            if repeat_no == '0':
                module_runtimes[module] = float(time)
                current_module = module
        elif module != 'MasterAnalyzer':
            module_runtimes[module] = float(time)
            current_module = module
    else:
        if len(toks) == 3:
            [time, progress, msg] = toks
            if progress == '100' and msg == 'Finished' and 'VEST' not in current_module:
                module_runtimes[current_module] = int(float(time) - module_runtimes[current_module])
                current_module = ''

modules = module_runtimes.keys()
modules.sort()

for module in modules:
    print module + '\t' + str(module_runtimes[module])
