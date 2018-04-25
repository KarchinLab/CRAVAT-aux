import os
import json
import requests
import base64
import MySQLdb

local_dir = 'd:\\dv2\\resource\\e_sets\\cravat_nci'
remote_url = 'http://www.ndexbio.org/v2/network/'
output_fn = 'd:\\dv2\\resource\\ndex-remote-only-hugos.txt'

def make_file ():
    network_basenames = os.listdir(local_dir)
    wf = open(output_fn, 'w')
    count = 0
    remote_hugos_all = {}
    for bn in network_basenames:
        network_id = bn.split('.')[0]
        count += 1
        print(count, network_id)
        f = open(os.path.join(local_dir, bn))
        ndex_data = json.loads('\n'.join(f.readlines()))
        hugos = ndex_data['ids']
        #print(hugos)
        local_hugos = {}
        for hugo in hugos:
            if hugo not in local_hugos:
                local_hugos[hugo] = True
        local_hugos = local_hugos.keys()
        local_hugos.sort()
        
        url = remote_url + network_id
        r = requests.get(url, headers={'Authorization': 'Basic ' + 
                                       base64.b64encode('cravat2017:cravat2017')})
        data = json.loads(r.text)
        for chunk in data:
            if 'nodes' in chunk:
                remote_hugos = [n['n'].split('_')[0] for n in chunk['nodes']]
                remote_hugos.sort()
                break
        
        local_only_hugos = []
        remote_only_hugos = []
        for hugo in local_hugos:
            if hugo not in remote_hugos:
                local_only_hugos.append(hugo)
        for hugo in remote_hugos:
            if hugo not in local_hugos:
                remote_only_hugos.append(hugo)
        
        for hugo in remote_only_hugos:
            remote_hugos_all[hugo] = True
    remote_hugos_all = remote_hugos_all.keys()
    remote_hugos_all.sort()
    wf.write('\n'.join(remote_hugos_all) + '\n')
    wf.close()

def get_hugo_conversion ():
    conn = MySQLdb.connect('karchin-db01.icm.jhu.edu', 'rachelk', 'chrislowe', 'uniProt')
    cursor = conn.cursor()
    f = open(output_fn)
    protein_names = [v.strip() + '_HUMAN' for v in f.readlines()]
    f.close()
    wf = open(output_fn + '.hugos', 'w')
    count = 0
    for protein_name in protein_names:
        protein_name = protein_name.strip()
        count += 1
        sql = 'select acc from displayId where val="' + protein_name + '"'
        #print(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None:
            continue
        acc = result[0]
        sql = 'select val from gene where acc="' + acc + '"'
        #print(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result == None:
            continue
        hugo = result[0]
        wf.write(protein_name + '\t' + hugo + '\n')
        print(count, '/', len(protein_names), protein_name, hugo)
    wf.close()

get_hugo_conversion()