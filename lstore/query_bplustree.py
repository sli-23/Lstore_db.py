import pickle
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
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        tree = self.table.index.indices[self.table.key]
        tree.delete(primary_key)
            
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        indirection = MAXINT
        self.table.num_records += 1
        rid = self.table.num_records #num of records
        
        curr_time = int(time()) 

        schema_encoding = '0' * self.table.num_columns
        schema_encoding = int.from_bytes(schema_encoding.encode(), byteorder='big') #int

        default_column = [indirection, rid, curr_time, schema_encoding]
        column = list(columns)
        default_column.extend(column)
        self.table.base_write(default_column)
            
        self.table.key_lst.append(columns[self.table.key]) # Using in sum

        #index
        for i, val in enumerate(column):
            if i == self.table.key : #in the first column, key = primary key, value = rid
                self.table.index.create_index(i, val, rid)
            else: 
                self.table.index.create_index(i, rid, val)

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        records = [] # if there are multiple records
        record = []

        # Check if the length of query_column is unequal to num_columns
        if len(query_columns) != self.table.num_columns:
            return records

        # Index
        rid = self.table.index.locate(self.table.key, index_value)[0]
        
        # TODO: update after finishing bufferpool
        # base record
        record.append(index_value)
        for col in range(self.table.num_columns):
            if col == self.table.key or query_columns[col] == 0:
                continue
            else:
                val = self.table.index.locate(col, rid)[0]
                record.append(val)

        # query_column
        for col, val in enumerate(query_columns):
            if val == 0:
                record[col] = None
            else:
                continue
        
        return [Record(rid, index_value, record)]


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        columns = list(columns)
        if len(columns) != self.table.num_columns:
            print('Detect Error')
            return False

        rid = self.table.index.locate(self.table.key, primary_key)[0]
        (multipage_range, page_range, record_index) = self.table.rid_base(rid)
        base_indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_range].pages[page_range].get(record_index) #bytes
        base_indirection_int = int.from_bytes(bytes(base_indirection), byteorder='big')

        #insert tail record

        #update index
        for col, value in enumerate(columns):
            if value == None:
                continue
            else:
                self.table.index.update_index(primary_key, col, value)


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range s
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        if aggregate_column_index == self.table.key:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            for rid in rids:
                sum += rid
        else:
            rids = self.table.index.locate_range(start_range, end_range)
            for rid in rids:
                value = self.table.index.locate(aggregate_column_index, rid)[0]
                sum += value
        
        return sum

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
