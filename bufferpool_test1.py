from email.mime import base
from operator import index
from pickle import NONE

from numpy import rec
from lstore.db import Database
from lstore.query_bplustree import Query
from lstore.config import *
from lstore.bplustree import BPlusTree
from lstore.index import Index
from random import choice, randint, sample, seed
from xmlrpc.client import MAXINT
import shutil

try:
    shutil.rmtree('Grades', ignore_errors=True)
except:
    pass


db = Database()
db.open('./ECS165')
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

base_page = grades_table.page_directory['base']

def base_record(index):
    column = []
    for i in range(len(base_page)):
        val = base_page[i][0].pages[0].get(index)
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column

tail_page = grades_table.page_directory['tail']

def tail_record(index):
    column = []
    for i in range(len(tail_page)):
        val = tail_page[i][0].get(index)
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column


# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 5
number_of_aggregates = 100
seed(3562901)
count = []
for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    count.append(records[key])
    query.insert(*records[key])
    print('inserted', records[key])
print("Insert finished")


count_update = 0
# update test
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # copy record to check
        original = records[key].copy()
        # update our test directory
        records[key][i] = value
        query.update(key, *updated_columns)
        count_update += 1
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
        else:
            #print(str(count_update) + ' update on', original, 'and', updated_columns, ':', record.columns)
            pass




"""
for i in range(3*number_of_records):
    print(tail_record(i))


for i in range(number_of_records):
    print('Original record:')
    records = base_record(i)
    print(records)

    for j in range(count_update):
        if tail_record(j)[0] == records[0]:
            print('tail_record:')
            print(tail_record(j))
            print('select key to check:')
            print(query.select(records[4], 0, [1, 1, 1, 1, 1])[0].columns)
            print('\n')
"""

db.close()
print(grades_table.bufferpool.last_rid)

for i in grades_table.bufferpool.lru_cache:
    print(i)