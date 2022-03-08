import pickle
import os
import lstore 

cwd = os.getcwd()
print(cwd)
fr = open(cwd + '/000.pkl', 'rb')
data = pickle.load(fr)
fr.close()

#check primary key
print(data)