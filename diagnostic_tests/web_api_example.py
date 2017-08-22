import requests

# GET
import requests
r=requests.get('http://dev.cravat.us/rest/service/submit', params={'email':'rkim@insilico.us.com', 'analyses':'', 'mutations':'TR1 chr22 30421786 + A T sample_1'})

# POST with an input file
r=requests.post('http://dev.cravat.us/rest/service/submit', files={'inputfile':open('input_file/vcf_input.txt')}, data={'email':'rkim@insilico.us.com','analyses':''})
