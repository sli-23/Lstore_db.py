from lstore import table
from lstore.db import Database
from lstore.query import Query
from lstore.page import Page, MultiPage

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)


# simple query(inserted)
records = {}

number_of_records = 10
number_of_aggregates = 100
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

records

# insert 1 record
records_test = [92106433, 1, 14, 1, 19]

# base_write simple test for inserting a record
test = Database()
table_test = test.create_table('Test', 5, 0)

table_test.base_write(records_test) #????

print(table_test.num_records) # base_write 了之后不增加records数量