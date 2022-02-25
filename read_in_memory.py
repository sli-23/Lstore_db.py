from lstore.BPlusTree import *
from random import choice, randint, sample, seed
from lstore.query_bplustree import Query
from lstore.db import Database
from lstore.config import *
from collections import OrderedDict
from lstore.bufferpool import Bufferpool


db = Database()
db.open('./ECS165')

grades_table = db.create_table('Grades', 5, 0)

query = Query(grades_table)

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

print(count)

