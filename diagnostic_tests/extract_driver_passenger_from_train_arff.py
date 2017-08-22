import sys

f = open(sys.argv[1])
wf_driver = open('drivers_from_old_classifier.tmps', 'w')
wf_passenger = open('passengers_from_old_classifier.tmps', 'w')

data_line = ''
for line in f:
    line = line.strip()
    if len(line) == 0:
        continue
    if line[0] == '@':
        continue
    data_line += line
    if line[-1] == '&':
        continue
    toks = data_line.split(' ')
    try:
        dummy = float(toks[1])
    except ValueError, e:
        t2 = toks[1].split('_')
        aa_change = t2[-1]
        transcript = '_'.join(t2[:-1])
        write_line = 'DummyID'+'\t'+transcript+'\t'+aa_change+'\n'
        mutation_class = toks[-1]
        if mutation_class == 'Driver':
            wf_driver.write(write_line)
#            print 'Driver', transcript, aa_change
        elif mutation_class == 'Passenger':
            wf_passenger.write(write_line)
#            print 'Passenger', transcript, aa_change
        data_line = ''
f.close()
wf_driver.close()
wf_passenger.close()