from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

print('Testing.....')
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# ---------------Insert---------------#
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
    # print('inserted', records[key])

print('-----------------')
print("Insert finished")
print('Insert Test')

# ---------------Insert test---------------#
page_range = round(1000 / 512)

if number_of_records != grades_table.num_records:
    raise ValueError('# records error')
elif page_range != grades_table.get_base_page_range():
    raise ValueError('# page range')
else:
    print(' - Query insert successes')
    print('-----------------')