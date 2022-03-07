
"""
A data structure holding indices for various columns of a table. 
Key column should be indexed by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.bplustree import BPlusTree
from lstore.config import *

class Tail_Index:
    def __init__(self, table):
        self.table = table
        self.index = BPlusTree(150) #key: primary key value: (newest_tail_indirection, tail rid)

    def locate(self, tail_indirection):
        return  self.index.retrieve(tail_indirection)

    def create_index(self, key, value):
        self.index.insert(key, value)


    def update(self, key, new_value):
        self.index.delete(key)
        self.index.insert(key, new_value)

    #TODO if merge then empty tail_index?

    
class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [BPlusTree(150) for _ in range(table.num_columns)]  # Give a default value for indices.

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column_number, key):
        tree = self.indices[column_number]
        return tree.retrieve(key)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column=None): #primary key - rids - values
        rids = []
        key_lst = sorted(self.table.key_lst)
        for key in key_lst:
            if key > end:
                break
            else:
                if key in range(begin, end + 1):
                    if column == self.table.key:
                        rids.append(key)
                    else:
                        rid = self.locate(self.table.key, key)[0]
                        rids.append(rid)
        return rids
    
    """
    # Update index
    """
    def update_index(self, key, column_number, new_value):
        rid = self.locate(self.table.key, key)[0]
        if column_number == self.table.key:
            tree = self.indices[self.table.key]
            tree.delete(key)
            tree.insert(new_value, rid)
        else:
            tree = self.indices[column_number]
            tree.delete(rid)
            tree.insert(rid, new_value)


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

    