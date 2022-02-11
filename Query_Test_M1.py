from lstore.db import Database
from lstore.query import Query
from lstore.config import *

from random import choice, randint, sample, seed

print('Testing Insert function.....')
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# ---------------Insert--------------- #
records = {}

number_of_records = 2
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

# ---------------Insert test--------------- #
page_range = round(number_of_records / 512)
if page_range == 0:
    page_range = 1

if number_of_records != grades_table.num_records:
    raise ValueError('# records error')
elif page_range != grades_table.get_base_page_range():
    raise ValueError('# page range')
else:
    print(' - Query insert successes')
    print('-----------------')

# ----------------Visualization--------------- #
visual_request = input('Visualization request? (y/n): ')

while visual_request != 'y' and visual_request != 'n':
    visual_request = input('Visualization request? (y/n): ')

if visual_request == 'y':
    col_number = int(input('Enter the # columns you want to check (start with 1): '))
    if col_number > grades_table.num_columns:
        raise ValueError
    index_number = int(input('Enter the index of this column you want to check (start with 0): '))
    if index_number > grades_table.num_records:
        raise ValueError
    
    multipage_test = round(index_number / 8192)
    test = grades_table.page_directory['base'][col_number][multipage_test]
    test_page_range = round(index_number / 512)
    index_number = index_number - multipage_test * 512
    test_page = test.pages[test_page_range]
    test_number = test_page.get(index_number)

    print('The original record is: ', count[index_number])
    print('The page range for this record is ', test_page_range + 1)
    print(int.from_bytes(bytes(test_number), byteorder='big'))

print('-----------------')

"""
#print a compete record with default columns
record_default_columns = []
#test for the first record

for i in range(0, 9):
    val = grades_table.page_directory['base'][i][0].pages[0].get(0)
    val = int.from_bytes(bytes(val), byteorder='big')
    record_default_columns.append(val)
    
print(record_default_columns)
"""

# ----------------Select--------------- #
('-----------------')
print('Testing select function.....')


# ----------------Update--------------- #
print('-----------------')
print('Testing update function.....')

for i in count:
    print(i)

base_column = []
for i in range(0,9):
    page = grades_table.page_directory['base'][i][0].pages[0].get(0)
    page = int.from_bytes(bytes(page), byteorder='big')
    base_column.append(page)

print('Before the first update: ', base_column)

updated_columns = [None, None, 0, None, None]
query.update(count[0][0], *updated_columns)


base_column = []
for i in range(0,9):
    page = grades_table.page_directory['base'][i][0].pages[0].get(0)
    page = int.from_bytes(bytes(page), byteorder='big')
    base_column.append(page)

print('After the first update: ', base_column)

tail_column = []
for i in range(0,9):
    record = grades_table.page_directory['tail'][i][0].get(0)
    record = int.from_bytes(bytes(record), byteorder='big')
    tail_column.append(record)

print('The tail page for the first update: ', tail_column)

print(grades_table.get_tail_columns(29233))


updated_columns = [None, None, None, 0, None]
query.update(count[0][0], *updated_columns)

base_column = []
for i in range(0,9):
    page = grades_table.page_directory['base'][i][0].pages[0].get(0)
    page = int.from_bytes(bytes(page), byteorder='big')
    base_column.append(page)

print('After the second update: ', base_column)

tail_column = []
for i in range(0,9):
    record = grades_table.page_directory['tail'][i][0].get(1)
    record = int.from_bytes(bytes(record), byteorder='big')
    tail_column.append(record)

print('The tail page for the second update: ', tail_column)

print(query.select(count[0][0], 0, [1,1,1,1,1])[0].columns)

# if inrection != maxint then it has update:
# tail's rid guide indirection
