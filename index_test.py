
from pickle import NONE
from lstore.db import Database
from lstore.query_bplustree import Query
from lstore.config import *
from lstore.bplustree import BPlusTree
from lstore.index import Index
from random import choice, randint, sample, seed

db = Database()
grades_table = db.create_table('Grades', 5, 0)
print(grades_table.rid_tail(512))

