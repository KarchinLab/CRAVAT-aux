import os
print os.getcwd()
a = os.path.normpath(os.path.join(os.getcwd(),os.path.pardir,'test_cases'))
print a
