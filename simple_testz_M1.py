# Apply in Milestone 1
import lstore
from lstore.query import Query

db = Database()
db.open('TEST')

test_table = db.create_table('Grades', 5, 0)
query = Query(test_table)

# insert, select, update, delete, and sum
query.insert(*args) 
query.select(*args)
query.update(*args)
query.sum(*args)
query.delete(*args)

db.close()