from lstore.table import Table
from lstore.bufferpool import *
from lstore.index import Index
from lstore.config import *
import time
import pickle
import os
import sys

class Database():

    def __init__(self):
        self.path = ""
        self.tables = {} 
        self.bufferpool = BufferPool() #the default buffer pool will be 1,000 pages or 4 MB
        self.primary_key = {}

    # TODO: bufferpool stored in disk / merge
    def open(self, path):
        try:
            self.path = path
            if not os.path.exists(path):
                os.makedirs(path)
            self.bufferpool.initial_path(path)
            tabledata_file = open(path + '/Tables', 'wb')
            tabledata_file.close()
            key_file = open(path + '/Primary_Key', 'wb')
            key_file.close()
        except:
            tabledata_file = open(path + '/Tables', 'rb')
            tabledata_file.close()
            key_file = open(path + '/Primary_Key', 'rb')
            key_file.close()

    def keydict(self, table_name, table):
        key = table.key
        column = table.page_directory['base'][DEFAULT_COLUMN + key]
        self.primary_key[table_name] = column

    def close(self):
        for name, table in self.tables.items():
            #table.close() #it will trigger merger and evict all
            self.keydict(name, table)
        
        for key in self.primary_key.keys():
            primary_k = self.primary_key[key]
            key_file = open(self.path + '/' + key + '.table_key', 'wb')
            pickle.dump(primary_k, key_file)
            key_file.close()
        
        for key in self.tables.keys():
            table = self.tables[key]
            tabledata_file = open(self.path + '/' + key + '.table', 'wb')
            pickle.dump(table, tabledata_file)
            tabledata_file.close()

        os.remove(self.path + '/' + 'Tables')
        os.remove(self.path + '/' + 'Primary_Key')


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
        path = self.path
        data = None
        for filename in os.listdir(path):
            if filename[:-6] == name:
                path = path + '/' + filename
                fr = open(path, 'rb')
                data = pickle.load(fr)
                fr.close()
        if data == None:
            print(f'table {name} not exists.')
        else:
            return data
                    
        """
        if name in self.tables.keys():  # Check whether table named "name" in tables, if not, print alert info,else return the Table object.
            return self.tables[name]
        print(f'table {name} not exists.')
        """
