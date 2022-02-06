"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from BTrees.OOBTree import OOBTree
from config import *

class Index:

    def __init__(self, table):
        self.table = table
        # One index for each table. All our empty tree object initially.
        self.indices = [OOBTree() for _ in range(table.num_columns)]  # Give a default value for indices.


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        """
        :param column: int, the key of table's column
        :param value: the value that we search in the tree
        :return: the index of the value in the tree
        """
        return self.indices[column].get(value, None)


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return list(self.indices[column].values(min=begin, max=end))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = OOBTree()     # initially

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number, key, index):
        # update value "index" to the key of No.column_number.
        self.indices[column_number].update({key: index})
    
    def set_index(self, column_number, key, index):
        # update value "index" to the key of No.column_number.
        self.indices[column_number].update({key: index})