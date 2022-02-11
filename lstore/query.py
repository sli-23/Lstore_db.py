from email.mime import base
from enum import EnumMeta
from re import X
from sys import byteorder
from tkinter.tix import MAX
from xmlrpc.client import MAXINT
from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from time import time
from lstore.page import Page, MultiPage


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
        pass
    
    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        try:
            indirection = MAXINT
            rid = self.table.num_records #num of records
            curr_time = int(time())
            schema_encoding = '0' * self.table.num_columns
            schema_encoding = int.from_bytes(schema_encoding.encode(), byteorder='big')

            default_column = [indirection, rid, curr_time, schema_encoding]
            column = list(columns)
            default_column.extend(column)
            self.table.base_write(default_column)

            self.table.key_lst.append(columns[self.table.key])
            return True
        except:
            return False

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

        records = []
        
        # Check if the length of query_column is unequal to num_columns
        if len(query_columns) != self.table.num_columns:
            return records
        
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
        


        record = []

        for col, data in enumerate(query_columns): 
            if data == 0:
                record.append(None)
            else:
                val = self.table.page_directory['base'][DEFAULT_COLUMN + col][page_range].pages[page_index].get(record_index)
                record.append(int.from_bytes(bytes(val), byteorder='big'))
        
        rid = self.table.get_base_rid(page_range, page_index, record_index)
        key = record[self.table.key]

        record_class = Record(rid, key, record)
        records.append(record_class)
        return records
   
    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        base_indirection = self.table.key_indirection(primary_key)
        tail_record_multipage, tail_record_page_range, tail_record_index = self.table.get_base(primary_key)
        columns = list(columns)
        for col, val in enumerate(columns):
            if val == None:
                continue
            else:
                # tail - rid
                tail_rid = int.from_bytes(('r' + str(self.table.num_updates)).encode(),byteorder = 'big')
                # first update
                if int.from_bytes(bytes(base_indirection), byteorder='big') == MAXINT:
                    tail_indirection = self.table.key_rid(primary_key) #bytes
                    tail_indirection = int.from_bytes(bytes(tail_indirection), byteorder='big')
                    
                    tail_column = []
                    tail_column = [MAXINT for i in range(0, len(columns))]
                #already updated
                else:
                    tail_indirection = int.from_bytes(base_indirection, byteorder='big')
                    tail_column = self.table.get_tail_columns(base_indirection)
                    tail_column[col] = val

                schema_encoding = ["0" for _ in range(self.table.num_columns)]
                schema_encoding[col] = '1'
                schema_encoding_new = ''
                for c in schema_encoding:
                    schema_encoding_new += c
                
                schema_encoding = int.from_bytes(("".join(schema_encoding_new)).encode(), byteorder='big')
                base_schema_encoding = int.from_bytes(self.table.get_schema_encoding_base(primary_key), byteorder='big')
                schema_encoding = schema_encoding|base_schema_encoding
                
                default_column = [tail_indirection, tail_rid, schema_encoding]
                default_column.extend(tail_column)
                
                #self.table.tail_write(default_column)
                for col, val in enumerate(default_column):
                    tail_page = self.table.page_directory['tail'][col][-1]
                    if not tail_page.has_capacity():
                        self.table.page_directory['tail'][col].append(Page())
                        tail_page = self.table.page_directory['tail'][col][-1]
                    tail_page.write(val)

                self.table.page_directory['base'][INDIRECTION_COLUMN][tail_record_multipage].pages[tail_record_page_range].updata(tail_record_index, tail_rid)
                self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][tail_record_multipage].pages[tail_record_page_range].updata(tail_record_index, schema_encoding)
                self.table.num_updates += 1


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        key_lst = sorted(self.table.key_lst)
        start_index = 0
        end_index = 0
        count = 0

        for key in key_lst:
            if key == start_range:
                start_index = count
            elif key == end_range:
                end_index = count
            count += 1
        
        keys = key_lst[start_index:end_index]
        sum = 0

        for key in keys:
            col = [0] * self.table.num_columns
            col[aggregate_column_index] = 1
            #sum += (self.select2(key, col))[0].columns[aggregate_column_index]
        
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
