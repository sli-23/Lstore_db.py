from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

print('Testing.....')
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

if len(count) < 20:
    for i in count:
        print(i)

if visual_request == 'y':
    col_number = int(input('Enter the # columns you want to check (start with 1): '))
    index_number = int(input('Enter the index of this column you want to check (start with 0): '))
    multipage_test = round(index_number / 8192)
    test = grades_table.page_directory['base'][3 + col_number][multipage_test]
    
    test_page_range = round(index_number / 512)
    index_number = index_number - multipage_test * 512
    test_page = test.pages[test_page_range]
    test_number = test_page.get(index_number)
    print('The original record is: ', count[index_number])
    print('The page range for this record is ', test_page_range + 1)
    print(int.from_bytes(bytes(test_number), byteorder='big'))

#print all columns with data base page

# ----------------Select--------------- #


# ----------------Select Test--------------- #



# ----------------Delete--------------- #

# ----------------Sum--------------- #