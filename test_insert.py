from lstore.db import Database
from lstore.query_bplustree import Query
from lstore.table import Table

from random import choice, randint, sample, seed

table = Table('test', 2, 0)

print(table.rid_base(1024)) #512 should be (0,1,0) rid start with 0
print(table.rid_tail(512))