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
        self.page_pointer = [0,0,0]
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

        base_indirection = self.table.bufferpool.get_record(self.table.name, INDIRECTION_COLUMN, multipage_id, page_range_id, record_id, 'Base_Page')
        base_indirection = int.from_bytes(base_indirection, byteorder='big')
        base_schema_encoding = self.table.bufferpool.get_record(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_range_id, record_id, 'Base_Page')
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

        #overwrite base_indirection + schema_encoding in bufferpool
        self.table.bufferpool.get_page(self.table.name, INDIRECTION_COLUMN, multipage_id, page_range_id, 'Base_Page').update(record_id, tail_indirection)
        self.table.bufferpool.get_page(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_range_id, 'Base_Page').update(record_id, new_schema)

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
        
        #Using in import Primary_key
        self.table.key_lst.append(columns[self.table.key]) 

        # update indices
        multipage_id = rid // (RECORDS_PER_PAGE * MAXPAGE)
        range_remainder = rid % (RECORDS_PER_PAGE * MAXPAGE)
        self.page_pointer = [int(multipage_id), int(range_remainder//RECORDS_PER_PAGE), int(range_remainder%RECORDS_PER_PAGE)]

        for i in range(self.table.num_columns):
            if self.table.index.indices[i] != None:
                self.table.index.update_index(columns[i], self.page_pointer, i)

        #bufferpool
        self.table.bufferpool.set_new_rid('base', rid)
        self.table.num_records += 1

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(self):
        pass

    def select(self, index_value, index_column, query_columns):
        page_pointer = self.table.index.locate(index_column, index_value)
        records_lst = [] # if there are multiple records
        multipage_id = page_pointer[0][0]
        page_range_id = page_pointer[0][1]
        record_id = page_pointer[0][2]

        self.table.page_locks.acquire_page_lock(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_range_id)
        base_schema_encoding = self.table.bufferpool.get_record(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_range_id, record_id, 'Base_Page')
        base_schema_encoding = int.from_bytes(base_schema_encoding, byteorder='big')

        self.table.page_locks.acquire_page_lock(self.table.name, INDIRECTION_COLUMN, multipage_id, page_range_id)
        base_indirection = self.table.bufferpool.get_record(self.table.name, INDIRECTION_COLUMN, multipage_id, page_range_id, record_id, 'Base_Page')
        base_indirection = int.from_bytes(base_indirection, byteorder='big')

        record = []
        for col, val in enumerate(query_columns):
            if val == 0:
                record.append(None) #可能要改
                continue
            if self.table.schema_update_check(base_schema_encoding, col): # has update
                tail_page_id, tail_record_id = self.table.rid_tail(base_indirection)
                data = self.table.bufferpool.get_tail_record(self.table.name, DEFAULT_COLUMN + col, tail_page_id, tail_record_id, 'Tail_Page')
                data = int.from_bytes(data, byteorder='big')
                record.append(data)
            else:
                #get base
                data = self.table.bufferpool.get_record(self.table.name, DEFAULT_COLUMN + col, multipage_id, page_range_id, record_id, 'Base_Page')
                data = int.from_bytes(data, byteorder='big')
                record.append(data)


        rid = self.table.bufferpool.get_record(self.table.name, DEFAULT_COLUMN + RID_COLUMN, multipage_id, page_range_id, record_id, 'Base_Page')
        rid = int.from_bytes(rid, byteorder='big')
        primary_key = self.table.bufferpool.get_record(self.table.name, DEFAULT_COLUMN + self.table.key , multipage_id, page_range_id, record_id, 'Base_Page')
        primary_key = int.from_bytes(primary_key, byteorder='big')
        records = Record(rid, primary_key, record)
        records_lst.append(records)

        self.table.page_locks.release_page_lock(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_range_id)
        self.table.page_locks.release_page_lock(self.table.name, INDIRECTION_COLUMN, multipage_id, page_range_id)

        return records_lst


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        if len(columns) != self.table.num_columns:
            print('Detect Error')
            return False

        page_pointer = self.table.index.locate(self.table.key, primary_key)

        multipage_range = page_pointer[0][0]
        page_range = page_pointer[0][1]
        record_index = page_pointer[0][2]

        #Acquire page lock

        # Meta data
        base_indirection = self.table.bufferpool.get_record(self.table.name, INDIRECTION_COLUMN, multipage_range, page_range, record_index, 'Base_Page')
        base_indirection = int.from_bytes(bytes(base_indirection), byteorder='big')
        base_rid = self.table.bufferpool.get_record(self.table.name, RID_COLUMN, multipage_range, page_range, record_index, 'Base_Page')
        base_rid = int.from_bytes(base_rid, byteorder='big')

        for col, val in enumerate(columns):
            if val == None:
                continue
            else:
                tail_rid = self.table.num_updates
                if base_indirection == MAXINT:
                    tail_indirection = base_indirection
                    tail_column = []
                    tail_column = [MAXINT for i in range(0, len(columns))]
                    #print(val)
                    tail_column[col] = val
                else:
                    tail_indirection = base_indirection
                    # get tail columns base_indirection = tid?
                    tail_column = self.table.get_tail_record(base_indirection)
                    tail_column[col] = val

                base_encoding = self.table.bufferpool.get_record(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_range, page_range, record_index, 'Base_Page')
                base_encoding = int.from_bytes(base_encoding, byteorder='big')
                tail_encoding = self.table.new_schema_encoding(base_encoding, col)
                
                curr_time = int(time())
                meta_data =[tail_indirection, tail_rid, curr_time, tail_encoding]
                meta_data.extend(tail_column)
                #print(meta_data)
                self.table.tail_write(meta_data)

                # Overwrite base page 
                self.table.bufferpool.get_page(self.table.name, INDIRECTION_COLUMN, multipage_range, page_range, 'Base_Page').update(record_index, tail_rid)
                self.table.bufferpool.get_page(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_range, page_range, 'Base_Page').update(record_index, tail_encoding)
                
                # release page latching

        # BufferPool and Merge
        self.table.bufferpool.set_new_rid('tail', tail_rid)
        self.table.mergeQ.append((base_rid, tail_rid))
        self.table.num_updates += 1
        
        # Check merge
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
        key_range = self.table.index.locate_range(start_range, end_range, self.table.key)
        for key in key_range:
            page_pointer = self.table.index.locate(self.table.key, key)
            multipage_id = page_pointer[0][0]
            page_id = page_pointer[0][1]
            record_id = page_pointer[0][2]

            base_indirection = self.table.bufferpool.get_record(self.table.name, INDIRECTION_COLUMN, multipage_id, page_id, record_id, 'Base_Page')
            base_indirection = int.from_bytes(bytes(base_indirection), byteorder='big')
            base_encoding = self.table.bufferpool.get_record(self.table.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_id, record_id, 'Base_Page')
            base_encoding = int.from_bytes(base_encoding, byteorder='big')

            if self.table.schema_update_check(base_encoding, aggregate_column_index): #has update
                page_range, record_index = self.table.rid_tail(base_indirection)
                val = self.table.bufferpool.get_tail_record(self.table.name, DEFAULT_COLUMN + aggregate_column_index, page_range, record_index, 'Tail_Page') 
                val = int.from_bytes(val, byteorder='big')
                sum += val
            else:
                val = self.table.bufferpool.get_record(self.table.name, DEFAULT_COLUMN + aggregate_column_index, multipage_id, page_id, record_id, 'Base_Page')
                val = int.from_bytes(val, byteorder='big')
                sum += val
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
