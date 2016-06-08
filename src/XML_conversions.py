import xml.etree.ElementTree as ET

# xml_to_dict will move recursively through an xml.etree.ElementTree object and transform it into a nested dictionary.
def ET_to_dict(d):
    out = {}
    if len(d):
        for k in d:
            if len(k.attrib):
                out[k.attrib['name']] = ET_to_dict(k)
            else:
                out[k.tag] = ET_to_dict(k)
    else:
        if d.text == None:
            out = ''
        else:
            out = d.text
            rep_dict = {'\\x20':'\x20'}
            for old in rep_dict:
                out = out.replace(old,rep_dict[old])
    return out

def xml_to_dict(f):
    if type(f) == 'str':
        path = f
        with open(path) as f:
            xml_raw = ET.parse(f).getroot()
    else:
        xml_raw = ET.parse(f).getroot()
    
    xml_dict = ET_to_dict(xml_raw)
    
    return xml_dict