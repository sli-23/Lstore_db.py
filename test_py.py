from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed


# dictionary for records to test the database: test directory
records = {}

number_of_records = 5
number_of_aggregates = 100
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    # print('inserted', records[key])
print("Insert finished")

# Check inserted records using select query

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(2, 5):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        print(updated_columns)
        original = records[key].copy()
        print(original)
        # update our test directory
        records[key][i] = value
