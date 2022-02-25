
from pickle import NONE
from lstore.db import Database
from lstore.query_bplustree import Query
from lstore.config import *
from lstore.BPlusTree import BPlusTree
from lstore.index_bplustree import Index
from random import choice, randint, sample, seed

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# ---------------Insert--------------- #
records = {}

number_of_records = 1000
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


# Check inserted records using select query
for key in records:
    # select function will return array of records 
    # here we are sure that there is only one record in t hat array
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
    else:
        pass

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
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None