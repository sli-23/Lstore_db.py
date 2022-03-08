from operator import index
import pickle
from tkinter.tix import MAX
from turtle import update
from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from lstore.bufferpool import BufferPool
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
        delete_val = [MAXINT for _ in range(self.table.num_columns)]

        base_rid = self.table.index.locate(self.table.key, primary_key)[0]
        multipage_id, page_range_id, record_id = self.table.rid_base(base_rid)
        base_indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_id].pages[page_range_id].get(record_id) #bytes
        base_indirection = int.from_bytes(base_indirection, byteorder='big')
        base_schema_encoding = self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][multipage_id].pages[page_range_id].get(record_id) #bytes
        base_schema_encoding = int.from_bytes(base_schema_encoding, byteorder='big')
        tail_rid = self.table.num_updates

        if base_indirection == MAXINT:
            base_indirection = self.table.new_base_indirection(base_indirection, base_rid, tail_rid)
            tail_indirection = base_indirection
        else:
            tail_indirection = base_indirection

        # update tail recording
        curr_time = int(time())
        new_schema = int('1'* self.table.num_columns, 2)
        default_column = [tail_indirection, tail_rid, curr_time, new_schema]
        default_column.extend(delete_val)
        self.table.tail_write(default_column)

        #overwrite base_indirection + schema_encoding
        self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_id].pages[page_range_id].update(record_id, tail_indirection)
        self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][multipage_id].pages[page_range_id].update(record_id, new_schema)

        #overwrite base_indirection + schema_encoding in bufferpool
        self.table.bufferpool.get_page(self.table.name, INDIRECTION_COLUMN, multipage_id, page_range_id, 'Base_Page').update(record_id, tail_indirection)
        self.table.bufferpool.get_page(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_range_id, 'Base_Page').update(record_id, new_schema)

        # delete index
        tree = self.table.index.indices[self.table.key]
        tree.delete(primary_key)

        self.table.num_updates += 1
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):    
        # Meta data
        indirection = MAXINT
        rid = self.table.num_records #num of records
        curr_time = int(time()) 
        schema_encoding = 0
        default_column = [indirection, rid, curr_time, schema_encoding]
        column = list(columns)
        default_column.extend(column)
        self.table.base_write(default_column)
        self.table.key_lst.append(columns[self.table.key]) #Using in import Primary_key

        #Update Index
        for i, val in enumerate(column):
            if i == self.table.key : #in the first column, key = primary key, value = rid
                self.table.index.create_index(i, val, rid)
            else: 
                self.table.index.create_index(i, rid, val)

        #bufferpool
        self.table.bufferpool.set_new_rid('base', rid)

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

        # Check if the length of query_column is unequal to num_columns
        if len(query_columns) != self.table.num_columns:
            return records

        # index_value will be always a key 
        # Using key to map the rid => page_indirection (multipage_id, page_range_id, record_index)
        rid = self.table.index.locate(self.table.key, index_value)[0]
        multipage_id, page_range_id, record_id = self.table.rid_base(rid)

        # ------------- SELECT DATA BY USING BufferPool ------------- #
        base_schema_encoding = self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][multipage_id].pages[page_range_id].get(record_id) #bytes
        base_schema_encoding = int.from_bytes(base_schema_encoding, byteorder='big')

        base_indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_id].pages[page_range_id].get(record_id) #bytes
        base_indirection = int.from_bytes(base_indirection, byteorder='big')
        
        record = []
        record.append(index_value)
        if base_indirection != MAXINT: #no updates
            lastest_tail_rid = self.table.tail_index.locate(index_value)[0][1]
            update_column = self.table.get_tail_record(lastest_tail_rid)
 
        for col, val, in enumerate(query_columns):
            if val == 0:
                record.append(None)
            if col != self.table.key:
                if self.table.schema_update_check(base_schema_encoding, col):
                    data = update_column[col]
                    record.append(data)
                else:
                    #no update - return to base data
                    data = self.table.index.locate(col, rid)[0]
                    record.append(data)
    
       
        #print(record)    
        # ------------- SELECT DATA BY USING INDEX ------------- #
        """
        record = []
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
            else: #check schema -> get tail_page from bufferpool
                continue
        """
        # ------------- SELECT DATA BY USING INDEX ------------- #
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

        base_rid = self.table.index.locate(self.table.key, primary_key)[0]
        (multipage_range, page_range, record_index) = self.table.rid_base(base_rid)
        base_indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_range].pages[page_range].get(record_index) #bytes
        base_indirection = int.from_bytes(bytes(base_indirection), byteorder='big')
        tail_rid = self.table.num_updates
        
        updated = True
        if base_indirection == MAXINT:
            updated = False
        else:
            updated = True
        
        temp = 0
        for col, val in enumerate(columns):
            if val == None:
                temp += 1
                continue
            else:
                if updated == False: #new update
                    base_indirection = self.table.new_base_indirection(base_indirection, base_rid, tail_rid)
                    tail_indirection = base_indirection
                    #update tail index
                    tail_id = (tail_indirection, tail_rid, base_rid)
                    indirection_id = (primary_key, base_rid, tail_rid)
                    self.table.tail_index.create_index(primary_key, tail_id)
                    self.table.indirection_index.create_index(tail_indirection, indirection_id)
                    tail_column = []
                    tail_column = [MAXINT for i in range(0, len(columns))]
                    tail_column[col] = val
                else: #update existed
                    tail_indirection = base_indirection
                    rid = self.table.tail_index.locate(primary_key)[0][1]
                    tail_column = self.table.get_tail_record(rid)
                    tail_column[col] = val
            
            #write tail_page
            base_encoding = self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][multipage_range].pages[page_range].get(record_index)
            base_encoding = int.from_bytes(base_encoding, byteorder='big')
            tail_encoding = self.table.new_schema_encoding(base_encoding, col)
        
        if temp == self.table.num_columns:
            tail_indirection = base_indirection
            tail_encoding = self.table.bufferpool.get_page(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_range, page_range, 'Base_Page').get(record_index)
            tail_encoding = int.from_bytes(tail_encoding, byteorder='big')
            tail_column = [MAXINT for _ in range(self.table.num_columns)]

        #update tail index
        if updated and temp != self.table.num_columns:
            tail_indirection = self.table.new_base_indirection(base_indirection, base_rid, tail_rid)
            tail_id = (tail_indirection, tail_rid, base_rid)
            indirection_id = (primary_key, base_rid, tail_rid)
            self.table.tail_index.update(primary_key, tail_id)
            self.table.indirection_index.create_index(tail_indirection, indirection_id)
        
        curr_time = int(time())
        default_column = [tail_indirection, tail_rid, curr_time, tail_encoding]
        default_column.extend(tail_column)

        #overwrite base_indirection + schema_encoding
        self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_range].pages[page_range].update(record_index, tail_indirection)
        self.table.page_directory['base'][SCHEMA_ENCODING_COLUMN][multipage_range].pages[page_range].update(record_index, tail_encoding)

        #overwrite base_indirection + schema_encoding in bufferpool
        self.table.bufferpool.get_page(self.table.name, INDIRECTION_COLUMN, multipage_range, page_range, 'Base_Page').update(record_index, tail_indirection)
        self.table.bufferpool.get_page(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_range, page_range, 'Base_Page').update(record_index, tail_encoding)

        self.table.tail_write(default_column)

        #Bufferpool - last_rid
        self.table.bufferpool.set_new_rid('tail', tail_rid)
        self.table.mergeQ.append(tail_rid)
        self.table.num_updates += 1

        #update index
        for col, value in enumerate(columns):
            if value == None:
                continue
            else:
                self.table.index.update_index(primary_key, col, value)
        
        #check merge
        self.table.mergetrigger()

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
