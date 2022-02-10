from email.mime import base
from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self.index = Index(table)
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        try:
            columns.delete(rid)
            self.num_records -= 1
            return True
        except:
            return False
    
    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        indirection = 0 #num of records
        rid = self.table.num_records #need updates
        curr_time = int(time())
        schema_encoding = int('0' * self.table.num_columns)
        column = list(columns)
        default_column = [indirection, rid, curr_time, schema_encoding]
        default_column.extend(column)
        data = default_column
        # index
        
        self.table.base_write(data)

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    # may update in the future its pretty slow...
    def select(self, index_value, index_column, query_columns):
        record = []
        
        # Check if the length of query_column is unequal to num_columns
        if len(query_columns) != self.table.num_columns:
            return record
        
        byte_value = index_value.to_bytes(8, byteorder='big')
        multipage = self.table.page_directory['base'][DEFAULT_COLUMN + index_column] 

        page_range = 0
        page_index = 0
        record_index = 0
        
        for i in range(len(multipage)): # page_range
            for j in range(len(multipage[i].pages)): #in which pages in a single multipage
                for z in range(multipage[i].pages[j].num_records): # numbers of records in single pages
                    if multipage[i].pages[j].get(z) == byte_value:
                        page_range = i
                        page_index = j
                        record_index = z

        for col, data in enumerate(query_columns):
            if data == 0:
                record.append(None)
            else:
                val = self.table.page_directory['base'][DEFAULT_COLUMN + col][page_range].pages[page_index].get(record_index)
                record.append(int.from_bytes(bytes(val), byteorder='big'))
        
        return record

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        table_key = self.table.key
        multipage = self.table.page_directory['base'][DEFAULT_COLUMN + table_key]
        indirection = self.table.page_directory['base'][INDIRECTION_COLUMN]


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            for i in range(start_range , end_range):
                sum1 = len(start_range, end_range)
        except:
            return False

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
