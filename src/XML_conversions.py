# _recurse_xml will move recursively through an xml.etree.ElementTree object and transform it into a nested dictionary.
def recurse_to_dict(d):
    out = {}
    if len(d):
        for k in d:
            if len(k.attrib):
                out[k.attrib['name']] = recurse_to_dict(k)
            else:
                out[k.tag] = recurse_to_dict(k)
    else:
        if d.text == None:
            out = ''
        else:
            out = d.text
            rep_dict = {'\\x20':'\x20'}
            for old in rep_dict:
                out = out.replace(old,rep_dict[old])
    return out