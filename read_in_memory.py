from lstore.bplustree import *
from random import choice, randint, sample, seed
from lstore.query_bplustree import Query
from lstore.db import Database
from lstore.config import *
from collections import OrderedDict
from lstore.bufferpool import BufferPool


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
key = []
for record in count:
    key.append(record[0])

bp = BufferPool()
bp.path = './ECS165'
path = bp.buffer_to_path('Grades', 1, 0, 0, 0)
column = grades_table.page_directory['base'][4][0].pages[0]

#insert 
for k in key:
    column.write(k)

bp.write_page(column, path)
bufferid = ('Grades', 1, 0, 0, 0)
#buffer id: table_name, column_id, multipage_id, page_range_id, page_id
bp.add_page(bufferid, default=False)
print(bp.page_bufferpool)
db.close()
