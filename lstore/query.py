from email.mime import base
from enum import EnumMeta
from re import X
from sys import byteorder
from tkinter.tix import MAX
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
        record_multipage, record_page_range, record_index = self.table.get_base(primary_key) #base page
        self.table.delete_rid_base(record_multipage, record_page_range, record_index) #delete base page's rid

        indirection_base_bytes = self.table.key_indirection(primary_key)
        indirection_base_ints = int.from_bytes(bytes(indirection_base_bytes), byteorder='big')

        #indirection of the primary key
        if indirection_base_ints == MAXINT: #NO UPDATE
            val = (0).to_bytes(8, byteorder='big')
            for i in range(DEFAULT_COLUMN, DEFAULT_COLUMN + self.table.num_columns):
                value = self.table.page_directory['base'][i][record_multipage].pages[record_page_range].get(record_index)
                value = val
        else:
            tail_page_range, tail_index = self.table.get_tail_rid(indirection_base_ints)
            self.table.delete_rid_tail(tail_page_range, tail_index)
            val = (0).to_bytes(8, byteorder='big')
            for i in range(DEFAULT_COLUMN, DEFAULT_COLUMN + self.table.num_columns):
                value = self.table.page_directory['tail'][i][tail_page_range].get(tail_index)
                value = val       

            self.table.num_updates -= 1    
        self.table.num_records -= 1

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

            # index
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
        records = [] # if there are multiple records
        record = []

        # Check if the length of query_column is unequal to num_columns
        if len(query_columns) != self.table.num_columns:
            return records

        index_value_bytes = index_value.to_bytes(8, byteorder='big')
        index_multipage = self.table.page_directory['base'][DEFAULT_COLUMN + index_column]

        multipage = 0
        page_range = 0
        record_index = 0

        for i in range(len(index_multipage)):
            for j in range(len(index_multipage[i].pages)):
                for z in range(index_multipage[i].pages[j].num_records):
                    if index_multipage[i].pages[j].get(z) == index_value_bytes:
                        multipage = i
                        page_range = j
                        record_index = z

        # then we can get key / indirection

        key = self.table.page_directory['base'][DEFAULT_COLUMN + self.table.key][multipage].pages[page_range].get(record_index) #bytes
        indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage].pages[page_range].get(record_index) #bytes
        indirection_int = int.from_bytes(bytes(indirection), byteorder='big')


        if indirection_int != MAXINT:
            tail_page_range, tail_record_index = self.table.base_ind_tail_rid(indirection_int)
            maxint_bytes = MAXINT.to_bytes(8, byteorder='big') 
            for i in range(DEFAULT_COLUMN, DEFAULT_COLUMN + self.table.num_columns):
                if self.table.page_directory['tail'][i][tail_page_range].get(tail_record_index) != maxint_bytes:
                    val = self.table.page_directory['tail'][i][tail_page_range].get(tail_record_index)
                    record.append(val)
                else:
                    val = self.table.page_directory['base'][i][multipage].pages[page_range].get(record_index)
                    record.append(val)

        else:
            for i in range(DEFAULT_COLUMN, DEFAULT_COLUMN + self.table.num_columns):
                val = self.table.page_directory['base'][i][multipage].pages[page_range].get(record_index)
                record.append(val)

        for col, data in enumerate(query_columns):
            if data == 0:
                record[col] = None
            else:
                # convert to int
                record[col] = int.from_bytes(bytes(record[col]), byteorder='big')
        
        key_int = record[self.table.key]
        rid = self.table.get_base_rid(multipage, page_range, record_index)

        record_class = Record(rid, key_int, record)
        records.append(record_class)
        
        return records
   
    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        self.table.num_updates += 1
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
                    tail_column[col] = val
                
                #if the record has already updated
                else:
                    tail_indirection = int.from_bytes(base_indirection, byteorder='big')
                    base_indirection_int = int.from_bytes(bytes(base_indirection), byteorder='big')
                    tail_column = self.table.get_tail_columns(base_indirection_int) #if the record has been updated the indrection will not be MAXINT
                    tail_column[col] = val


                schema_encoding = ["0" for _ in range(self.table.num_columns)]
                schema_encoding[col] = '1'
                schema_encoding_new = ''
                for c in schema_encoding:
                    schema_encoding_new += c
                
                schema_encoding = int.from_bytes(("".join(schema_encoding_new)).encode(), byteorder='big')
                base_schema_encoding = int.from_bytes(self.table.get_schema_encoding_base(primary_key), byteorder='big')
                
                tail_schema_encoding = schema_encoding|base_schema_encoding
                curr_time = int(time())
                
                default_column = [tail_indirection, tail_rid, curr_time, tail_schema_encoding]
                default_column.extend(tail_column)
                
                #self.table.tail_write(default_column)
                for col, val in enumerate(default_column):
                    tail_page = self.table.page_directory['tail'][col][-1]
                    if not tail_page.has_capacity():
                        self.table.page_directory['tail'][col].append(Page())
                        tail_page = self.table.page_directory['tail'][col][-1]
                    tail_page.write(val)

                self.table.page_directory['base'][INDIRECTION_COLUMN][tail_record_multipage].pages[tail_record_page_range].updata(tail_record_index, tail_rid)
                self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][tail_record_multipage].pages[tail_record_page_range].updata(tail_record_index, tail_schema_encoding)
                

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
        
        key_sum = key_lst[start_index : end_index + 1]
        total_sum = 0
        
        for key in key_sum:
            query_column = [0] * self.table.num_columns
            query_column[aggregate_column_index] = 1
            val = self.select(key, 0, query_column)[0].columns[aggregate_column_index]
            total_sum += val

        return total_sum

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
