from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# Check how many columns
print(grades_table.num_columns)

# Check base pages default + # of columns
for base_page in grades_table.page_directory['base']:
    print(base_page)

# Check tail pages default + # of columns
for tail_page in grades_table.page_directory['tail']:
    print(tail_page)

# Check key columns
print(grades_table.key)


