from email.mime import base
from heapq import merge
from operator import index
from pickle import NONE
from signal import raise_signal

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
        val = tail_page[i][-1].get(index)
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column


# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 10
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
    #print('inserted', records[key])


keys = sorted(list(records.keys()))
print("Insert finished")

# Check inserted records using select query
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record.columns, ', correct:', records[key], record.rid)
        break
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

for i in range(number_of_records):
    print(base_record(i))

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
            print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
            #raise ValueError
        else:
            #print(str(count_update) + ' update on', original, 'and', updated_columns, ':', record.columns)
            pass

db.close()

"""
def buffer_tail(index):
    #table_name, column_id, page_range_id, record_id, base_or_tail
    column = []
    for i in range(len(tail_page)):
        val = grades_table.bufferpool.get_tail_record('Grades', i, 1, index, 'Tail_Page')
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column
"""
def buffer_base(index):
    column = []
    for i in range(len(base_page)):
        val = grades_table.bufferpool.get_record('Grades', i, 0, 0, index, 'Base_Page')
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column

"""
for i in range(number_of_records):
    print(base_record(i))


for i in range(3*number_of_records):
    col = tail_record(i)
    if col == [0, 0, 0, 0, 0, 0, 0, 0, 0]:
        break
    print(col)

"""
#print(grades_table.bufferpool.get_base_range())
#print(grades_table.bufferpool.get_page('Grades', 8, 0, 0, 'Base_Page').num_records)

