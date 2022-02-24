import pickle
import os
import lstore 

cwd = os.getcwd()
fr = open(cwd + '/Primary_Key', 'rb')
data = pickle.load(fr)
print(data)

fr = open(cwd + '/Tables', 'rb')
data = pickle.load(fr)
print(data)