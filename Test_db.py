from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 10

seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
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
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")