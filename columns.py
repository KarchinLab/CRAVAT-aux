import os
import random

tests = os.listdir('test_cases')
header = ['UF','US','EF','ES']
col_nums = {}
for h in header:
	col_nums[h] = header.index(h)+1
	
head_start = 'TESTS'
col_start = 25
col_space = 10
head_text = head_start.ljust(col_start)
spaces = 10
spaces = 10

with open('columns.txt','w') as f:
	# head_text += header[0].ljust(tabs_in, '-')
	for h in header:
		head_text += h.rjust(col_space)
	f.write(head_text+'\n')
	for t in tests:
		col = random.choice(header)
		dist_in = col_start + col_nums[col]*col_space - len(t)
		f.write(t)
		f.write('x'.rjust(dist_in) + '\n')		