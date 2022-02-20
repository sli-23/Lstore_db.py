
"""
A data structure holding indices for various columns of a table. 
Key column should be indexed by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.BPlusTree import * 
from lstore.config import *

class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [BPlusTree(10) for _ in range(table.num_columns)]  # Give a default value for indices.

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column_number, key):
        tree = self.indices[column_number]
        return tree.retrieve(key)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number, key, value):
        tree = self.indices[column_number]
        tree.insert(key, value)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number, key):
        tree = self.indices[column_number]
        tree.delete(key)

    def update_value(self, column_number, primary_key, new_value): #by using primary_key to update value #cannot update primary key in the table
        rid = self.locate(column_number, primary_key)
        old_value = self.locate(column_number, rid)
        if old_value == new_value:
            return
        else:
            self.indices[column_number].delete(rid)
            self.indices[column_number].insert(rid, new_value)
