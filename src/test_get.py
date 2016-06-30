import requests
import os
import json

path = os.path.join('C:\\','Users','Kyle','cravat','testing','test_cases','get_load','get_load_c','get_load_c_input.txt')
with open(path) as f:
    lines = f.read().split('\n')
line = lines[3]
line = '_'.join(line.split('\t')[1:])
url = 'http://192.168.99.100:8889/CRAVAT'
get_url = '/rest/service/query?mutation='
req_url = url + get_url + line
r = requests.get(req_url)
out = json.loads(r.content)
