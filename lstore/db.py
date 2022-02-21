from lstore.table import Table
from lstore.bufferpool import *
from lstore.index_bplustree import BPlusTree
import time
import pickle #only can be used in meta data
from os import path, mkdir, remove
import sys

class Database():

    def __init__(self):
        self.path = ""
        self.tables = {}  # Use a hash map to store tables.
        self.bufferpool = Bufferpool()

    # TODO: bufferpool stored in disk / merge
    def open(self, path):
        pass

    def close(self):
        # close bufferpool (evict pages in bufferpool)
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables.keys():
            print(f'table "{name}" exists...')
            table = self.tables[name]
        else:
            table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name not in self.tables.keys():      # Check whether table named "name" in tables, if not, print alert info,else delete the table.
            print(f'table {name} not exists.')
            return
        del self.tables[name]

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name in self.tables.keys():  # Check whether table named "name" in tables, if not, print alert info,else return the Table object.
            return self.tables[name]
        print(f'table {name} not exists.')
