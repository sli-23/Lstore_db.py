from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

print('Testing Insert function.....')
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# ---------------Insert--------------- #
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
    test = grades_table.page_directory['base'][3 + col_number][multipage_test]
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

count_error = 0
for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])
    error = False
    for i, column in enumerate(record):
        if column != records[key][i]:
            error = True
            count_error += 1
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)

if count_error != 0:
    raise ValueError('error occurs')
else:
    print(' - Testing completed')

# ----------------Update--------------- #
print('-----------------')
print('Testing update function.....')


# ----------------Sum--------------- #
print('-----------------')
print('Testing sum function.....')


# ----------------Delete--------------- #
print('-----------------')
print('Testing delete function.....')
