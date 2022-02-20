
from lstore.db import Database
from lstore.query import Query
from lstore.config import *
from lstore.BPlusTree import BplusTree, Node, printTree
from lstore.index_bplustree import Index
from random import choice, randint, sample, seed

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# ---------------Insert--------------- #
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
    print('inserted', records[key])


index = Index(grades_table)
key_col = index.indices[DEFAULT_COLUMN + 0] # key + rid(value)
for i in range(len(count)):
    key_col.insert(str(count[i][0]), str(i))

print(key_col.search(str(92106435)))


"""
record_len = 3
bplustree = BplusTree(record_len)
bplustree.insert('5', '33')
bplustree.insert('15', '21')
bplustree.insert('25', '31')
bplustree.insert('35', '41')
bplustree.insert('45', '10')

printTree(bplustree)
"""

