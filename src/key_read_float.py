import csv

def attempt_round(entry,precision):
    try: out = round(float(entry),precision)
    except: out = entry
    return out

key = {}
sig_figs = 6
# Read the key file into a 2D dictionary
key_path = 'C:\\Users\\Kyle\\cravat\\testing\\test_cases\\pop_stats\\pop_stats_key.csv'
with open(key_path) as r:
    key_csv = csv.DictReader(r)
    for row in key_csv:
        uid = row['uid']
        key[uid] = row
        del key[uid]['uid']
        for entry in key[uid]:
            key[uid][entry] = attempt_round(key[uid][entry],sig_figs)

print key['CYP19A1']['esp_avg']