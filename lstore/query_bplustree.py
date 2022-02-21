import enum
from webbrowser import MacOSX
from lstore.table import Table, Record
from lstore.index_bplustree import Index
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
        pass
    
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
        schema_encoding = int.from_bytes(schema_encoding.encode(), byteorder='big')

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
        # TODO: if index_val is not the key
        rid = self.table.index.locate(index_column, index_value)
        if len(rid) == 0:
            return False
        rid = rid[0]

        # Using rid to find page directory: (multipage)(page range)(page index)
        multipage_range, page_range, page_index = self.table.rid_base(rid)
        
        # Base - Indirection
        indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_range].pages[page_range].get(page_index) #bytes
        indirection_int = int.from_bytes(bytes(indirection), byteorder='big')

        # Base page
        record.append(index_value) 
        for col in range(1, self.table.num_columns):
            val = self.table.index.locate(col, rid)[0]
            record.append(val)

        # TODO: update after finishing bufferpool
        # If has recent record / updates
        if indirection_int != MAXINT: #has updates
            pass
        
        # query_column
        for col, val in enumerate(query_columns):
            if val == 0:
                record[col] = None
            else:
                continue

        record_class = Record(rid, index_value, record)
        records.append(record_class)
        
        return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index): #using index
        pass

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
