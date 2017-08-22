import xml.etree.ElementTree as ET
import os
from test_engine import parse_test_list

def addElement(tree, new_elem_name, new_elem_text, overwrite):
    if len(tree.findall(new_elem_name)) == 1:
        if overwrite == True:
            print 'Element already exists.\n Current text: "%s"\n Overwriting with: "%s"' %(tree.find(new_elem_name).text, new_elem_text)
            tree.find(new_elem_name).text = new_elem_text
        elif overwrite == False:
            print 'Element already exists. Skipping'
    elif len(tree.findall(new_elem_name)) > 1:
        print 'Multiple instances of element %s found. Skipping' %new_elem_name
    else:
        new_elem = ET.Element(new_elem_name)
        new_elem.text = new_elem_text
        tree.getroot().append(new_elem)
    return tree  

if __name__ == '__main__':
    test_cases = ['get_load\\c'] # Input tests to run as list of strings, or use 'all' to run every test in directory
    exclude_cases = [];
    test_cases_dir = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases'))
    
    test_list = parse_test_list(test_cases,test_cases_dir)
    for test in test_list:
        test_dir = os.path.join(test_cases_dir,test)
        print test_dir.split('\\')[-1]
        desc_path = os.path.join(test_dir,'%s_desc.xml' %test_dir.split('\\')[-1])
        ## Things to do to desc files go here ###
        desc = ET.parse(desc_path)
        desc = addElement(desc,'sub_method','get',True)
        desc.write(desc_path)