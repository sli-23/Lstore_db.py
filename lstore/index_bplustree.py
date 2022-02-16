
"""
A data structure holding indices for various columns of a table. 
Key column should be indexed by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

from lstore.bplustree import BplusTree, Node
from lstore.config import *

class Index:

    def __init__(self, table):
        # RID == number of records
        self.table = table
        self.indices = [BplusTree(5) for _ in range(table.num_columns)]  # Give a default value for indices.
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass