import pickle
import os
import lstore 

cwd = os.getcwd()
fr = open(cwd + '/Grades.table', 'rb')
data = pickle.load(fr)

table = data
fr.close()

print(table.num_records)
