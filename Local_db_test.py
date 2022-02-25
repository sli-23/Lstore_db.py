import pickle
import os
import lstore 

cwd = os.getcwd()
fr = open(cwd + '/Primary_Key.pkg', 'rb')
data = pickle.load(fr)
print(data)

fr = open(cwd + '/Tables.pkg', 'rb')
data = pickle.load(fr)
print(data)

fr = open(cwd + '/1000.pkg', 'rb')
data = pickle.load(fr)
print(data.data)