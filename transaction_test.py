from sre_constants import CHCODES
from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primary key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)


base_page = grades_table.page_directory['base']
def buffer_base(index):
    column = []
    for i in range(len(base_page)):
        val = grades_table.bufferpool.get_record('Grades', i, 0, 0, index, 'Base_Page')
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column

tail_page = grades_table.page_directory['tail']

def buffer_tail(index):
    #table_name, column_id, page_range_id, record_id, base_or_tail
    column = []
    for i in range(len(tail_page)):
        val = grades_table.bufferpool.get_tail_record('Grades', i, 0, index, 'Tail_Page')
        val = int.from_bytes(val, byteorder='big')
        column.append(val)
    return column

# dictionary for records to test the database: test directory
records = {}

number_of_records = 40
number_of_aggregates = 100
number_of_updates = 5

seed(3562901)
count = []
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    count.append(records[key])
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
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")


# x update on every column
for _ in range(number_of_updates):
    for key in keys:
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
                print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
                print(buffer_base(record.rid))
                raise ValueError
                #print(record.key, record.rid)
                #print(buffer_base(record.rid))
            else:
                pass
                # print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
print("Update finished")

for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    else:
        pass
        # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print("Aggregate finished")

db.close()



"""
print(grades_table.mergeQ)

for i in range(number_of_records):
    col = buffer_base(i)
    if col == [0, 0, 0, 0, 0, 0, 0, 0, 0]:
        break
    print(col)


for i in range(grades_table.num_updates):
    col = buffer_tail(i)
    if col == [0, 0, 0, 0, 0, 0, 0, 0, 0]:
        break
    print(col)

grades_table.merge2()

for i in range(number_of_records):
    col = buffer_base(i)
    if col == [0, 0, 0, 0, 0, 0, 0, 0, 0]:
        break
    print(col)


print(grades_table.mergeQ)
print(grades_table.get_merged_base_page())

for i in range(grades_table.num_columns):
    base_page_range = grades_table.get_merged_base_range(4 + i)
    print(base_page_range)
"""

