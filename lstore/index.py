
"""
A data structure holding indices for various columns of a table. 
Key column should be indexed by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.bplustree import BPlusTree
from lstore.config import *

class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [None for _ in range(table.num_columns)]  # Give a default value for indices.
        self.indices[self.table.key] = BPlusTree(150)
        self.tree = BPlusTree(150)

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
    def update_value(self, key, column_number, new_value):
        rid = self.locate(self.table.key, key)[0]
        if column_number == self.table.key:
            tree = self.indices[self.table.key]
            tree.delete(key)
            tree.insert(new_value, rid)
        else:
            tree = self.indices[column_number]
            tree.delete(rid)
            tree.insert(rid, new_value)

    def update_index(self, column_number, key, new_value):
        if not self.indices[column_number].has_key(key):
            self.indices[column_number].insert(key, new_value)
        else:
            self.update_value(key, column_number, new_value)

    """
    # optional: Create index on specific column
    """

    def create_value(self, column_number, key, value):
        tree = self.indices[column_number]
        tree.insert(key, value)

    def create_index(self, column_number): # Using table value to create a index
        self.table.create_table_index(column_number)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number, key):
        tree = self.indices[column_number]
        tree.delete(key)

class Tail_Index:
    def __init__(self, table):
        self.table = table
        self.index = BPlusTree(100) #key: primary key value: (newest_tail_indirection, tail rid, base rid)

    def locate(self, primary_key):
        return  self.index.retrieve(primary_key)

    def create_index(self, key, value):
        self.index.insert(key, value)

    def update(self, key, new_value):
        self.index.delete(key)
        self.index.insert(key, new_value)

    #TODO if merge then empty tail_index?

class Indirection_Index:
    def __init__(self, table):
        self.table = table
        self.index = BPlusTree(150) #key: indirection value: (primary key, base rid, tail)

    def locate(self, indirection):
        return  self.index.retrieve(indirection)

    def create_index(self, indirection, indirection_id):
        self.index.insert(indirection, indirection_id)

    def update(self, key, new_value):
        self.index.delete(key)
        self.index.insert(key, new_value)