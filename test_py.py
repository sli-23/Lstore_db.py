from lstore.index import Index
from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from random import choice, randint, sample, seed

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
index = Index(grades_table)

print(grades_table)
records = {}

number_of_records = 512
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

"""
# Check inserted records using select query
for key in records:
    # select function will return array of records 
    # here we are sure that there is only one record in t hat array
    record = query.select(key, 0, [1, 1, 1, 1, 1])
    error = False
    for i, column in enumerate(record):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
"""


print(grades_table.get_record_info(998))
