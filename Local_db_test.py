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