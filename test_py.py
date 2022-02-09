from lstore.index import Index
from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from random import choice, randint, sample, seed

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
index = Index(grades_table)

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
    # print('inserted', records[key])


print(count)

test = grades_table.page_directory['base'][-1][-1] #第几个column里面的multipage
test = test.pages[0]#multipage里面的第几个page
print(test)
b = test.get(0) #page里面的第几个
print(bytes(b))
print(int.from_bytes(b'\x00\x00\x00\x00\x00\x00\x00\x13', byteorder='big'))