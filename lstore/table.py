from concurrent.futures import thread
from heapq import merge
from lstore.bufferpool import BufferPool
from lstore.index import Index, RID_Index
from lstore.config import *
from lstore.page import Page, MultiPage
from time import time
import threading
from collections import deque, defaultdict
from copy import copy
from lstore.transaction import LockManager
from lstore.Page_Lock import PageLocks, RidLocks
from random import choice, randint, sample, seed
from lstore.bplustree import BPlusTree

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_records = 0
        self.num_updates = 0

        # Page directory violate
        self.page_directory = {}
        self.__init_page_directory()

        # Indexing 1
        self.index = Index(self)
        self.index.create_index(self.key)
        # Indexing 2
        self.rid_index = RID_Index(self)
        
        # BufferPool
        self.bufferpool = BufferPool()

        # Merge
        self.key_lst = []
        self.mergeQ = deque() #(base rid: tail rid)
        self.closed = False
        
        # Lock Manager
        self.lock_manager = LockManager()
        self.page_locks = None
        self.rid_locks = None
        self.__init_lock()
    
    def __init_page_directory(self):
        """
        Give a default page/multipage obj to page_directory
        :return:

        page directory should map RID to page range, page....
        """
        self.page_directory = {'base':[],'tail':[]}
        for i in range(self.num_columns + DEFAULT_COLUMN):
            self.page_directory['base']=[[MultiPage()] for _ in range(self.num_columns + DEFAULT_COLUMN)]
            self.page_directory['tail'] = [[Page()] for _ in range(self.num_columns + DEFAULT_COLUMN)]

    def __init_lock(self):
        self.page_locks = PageLocks()
        self.rid_locks = RidLocks()

    def set_merge_queue(self):
        base_page_range = 1 #update
        for col in range(self.num_columns):
            self.merge_queue_matrix.append([])
            for i in range(base_page_range):
                self.merge_queue_matrix[col].append(deque())

    def get_tail_range(self):
        # get current tail range from mergeQ
        start_rid = self.mergeQ[0][1]
        end_rid = self.mergeQ[-1][1]
        start_range, record_id = self.rid_tail(start_rid)
        end_range, record_id = self.rid_tail(end_rid)
        
        return start_range, end_range + 1

    # merge function        
    # https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System
    # check page 6 for details

    def mergetrigger(self, merge_trigger = False):
        if self.num_updates % MERGE_TRIGGER == 0:
            merge_thread = threading.Thread(target=self.merge())
            merge_thread.start()
            merge_thread.join()
        if merge_trigger == True:
            merge_thread = threading.Thread(target=self.merge())
            merge_thread.start()
            merge_thread.join()

    def merged_base_range(self, column_id):
        page_range = {}
        last_base_rid = self.bufferpool.last_rid['base']
        multipage_id, page_range_id, record_id = self.rid_base(last_base_rid)

        for multipage in range(multipage_id + 1):
            if multipage == multipage_id:
                for page_id2 in range(page_range_id + 1):
                    buffer_id = (self.name, column_id, multipage, page_id2, 'Base_Page')
                    page = self.bufferpool.get_page(self.name, column_id, multipage, page_id2, 'Base_Page')
                    page_range[buffer_id] = page
                break
            for page_id in range(MAXPAGE):
                buffer_id = (self.name, column_id, multipage, page_id, 'Base_Page')
                page = self.bufferpool.get_page(self.name, column_id, multipage, page_id, 'Base_Page')
                page_range[buffer_id] = page
        return page_range

    def merge(self):
        #print('Starting merging....')
        for column_id in range(DEFAULT_COLUMN, DEFAULT_COLUMN + self.num_columns):
            base_page_range = self.merged_base_range(column_id)
            base_page_range_copy = copy(base_page_range)

            # Merging by using MergeQ
            update = {}
            for tuple in list(self.mergeQ):
                base_rid = tuple[0]
                tail_rid = tuple[1]
                update[base_rid] = tail_rid
            
            for base_rid, tail_rid in update.items():
                tail_page_range_id, tail_record_id = self.rid_tail(tail_rid)
                tail_indirection = self.bufferpool.get_tail_record(self.name, INDIRECTION_COLUMN, tail_page_range_id, tail_record_id, 'Tail_Page')
                tail_indirection = int.from_bytes(tail_indirection, byteorder='big')

                multipage_id, base_page_range_id, base_record_id = self.rid_base(base_rid)
                buffer_id_base = (self.name, column_id,multipage_id, base_page_range_id, 'Base_Page')
                updated_val = self.bufferpool.get_tail_record(self.name, column_id, tail_page_range_id, tail_record_id, 'Tail_Page')
                updated_val = int.from_bytes(updated_val, byteorder='big')

                if updated_val != MAXINT:
                    page = base_page_range_copy[buffer_id_base]
                    page.update(base_record_id, updated_val)
                    old_schema = self.bufferpool.get_page(self.name ,SCHEMA_ENCODING_COLUMN, multipage_id, base_page_range_id, 'Base_Page').get(base_record_id)
                    old_schema = int.from_bytes(old_schema, byteorder='big')
                    new_schema = self.update_schema_encoding(column_id,old_schema)
                    self.bufferpool.get_page(self.name, SCHEMA_ENCODING_COLUMN, multipage_id, base_page_range_id, 'Base_Page').update(base_record_id, new_schema)
            if self.closed:
                break
            self.bufferpool.merge_base_range(base_page_range_copy)
        self.mergeQ.clear()

        return

    def update_schema_encoding(self, column_index, old_schema):
        old_schema = bin(old_schema)[2:].zfill(self.num_columns)
        new_schema = int(old_schema[:self.num_columns-(column_index - DEFAULT_COLUMN)-1] + "0" + old_schema[self.num_columns-(column_index - DEFAULT_COLUMN):], 2)
        return new_schema

    def base_write(self, data):
        for i, value in enumerate(data):
            multipage_id, page_range_id, record_id = self.rid_base(self.num_records)
            multiPages = self.page_directory["base"][i][-1]

            page1 = multiPages.get_current()
            page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            if not multiPages.last_page(): #If the page is not at the end of the page
                if not page1.has_capacity() and not page2.has_capacity(): #If the page is full
                    self.page_directory['base'][i][-1].add_page_index()
                    page1 = multiPages.get_current()

                    page_range_id += 1 #move to the next page range
                    page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            else: #If the page is the last page in the multipage
                if not page1.has_capacity() and not page2.has_capacity(): #if the last page in the multipage is full then
                    self.page_directory['base'][i].append(MultiPage())
                    page1 = self.page_directory['base'][i][-1].get_current()

                    multipage_id += 1 #move to the next multipage
                    page_range_id = 0 #reset the page range in the multipage
                    page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')

            page1.write(value)
            page2.write(value)
            page1.dirty = True
            page2.dirty = True

    def tail_write(self, data):
        for col, val in enumerate(data):
            page_id, record_index = self.rid_tail(self.num_updates)
            tail_page1 = self.page_directory['tail'][col][-1]
            tail_page2 = self.bufferpool.get_tail_page(self.name, col, page_id, 'Tail_Page')
        
            if not tail_page1.has_capacity() and not tail_page2.has_capacity():
                self.page_directory['tail'][col].append(Page())
                tail_page1 = self.page_directory['tail'][col][-1]

                page_id += 1
                tail_page2 = self.bufferpool.get_tail_page(self.name, col, page_id, 'Tail_Page')
            
            tail_page1.write(val)
            tail_page2.write(val)
            tail_page1.dirty = True
            tail_page2.dirty = True
            
    def rid_base(self, rid):
        record_multipage = 0
        record_multipage = rid // (RECORDS_PER_PAGE * MAXPAGE)
        rid = rid - RECORDS_PER_PAGE * MAXPAGE * record_multipage
        record_page_range = 0
        record_page_range = (rid // RECORDS_PER_PAGE)
        rid = rid - (RECORDS_PER_PAGE * record_page_range)
        record_index = rid
        return int(record_multipage), int(record_page_range), int(record_index)

    def rid_tail(self, rid):
        page_range = 0
        record_index = 0
        page_range = (rid // RECORDS_PER_PAGE)
        rid = rid - (RECORDS_PER_PAGE * page_range)
        record_index = rid      

        return int(page_range), int(record_index)

    def schema_encoding(self, column):
        schema_encoding = ''
        for col, val in enumerate(column):
            if val == None:
                schema_encoding += '0'
            else:
                schema_encoding += '1'
        return schema_encoding

    def new_schema_encoding(self, old_schema, query_column):
        return old_schema | (1<<query_column)

    def schema_update_check(self, schema, query_column):
        return (schema & (1<<query_column))>>query_column == 1 #false -> no update

    def new_base_indirection(self, base_indirection,base_rid,tail_rid):
        return base_indirection - tail_rid - base_rid - 1 - randint(0, 200)

    # get data from bufferpool
    def get_tail_record_data(self, tail_rid, column):
        page_range, record_index = self.rid_tail(tail_rid) #
        val = self.bufferpool.get_tail_record(self.name, column, page_range, record_index, 'Tail_Page') #bytes
        val = int.from_bytes(val, byteorder='big')
        return val

    # using tail_rid to get record from bufferpool
    def get_tail_record(self, tail_rid):
        column = []
        for i in range(self.num_columns):
            val = self.get_tail_record_data(tail_rid, 4 + i)
            column.append(val)
        return column

    def get_base_record_data(self, base_rid, column):
        multipage_id,page_range, record_index = self.rid_base(base_rid)
        val = val = self.bufferpool.get_record(self.name, column, multipage_id, page_range, record_index, 'Base_Page') #bytes
        val = int.from_bytes(val, byteorder='big')
        return val
    
    def get_base_record(self, base_rid):
        column = []
        for i in range(self.num_columns):
            column.append(self.get_base_record_data(base_rid, 4 + i))
        return column

    def create_table_index(self, column_number):
        tree = self.index.tree
        # create index on the non primary columns
        self.index.indices[column_number] = tree
        # create index on the non primary columns
        if self.index.indices[column_number] == True:
            print("Index existed")
            return
        
        for base_rid in range(self.num_records):
            multipage_id, page_id, record_id = self.rid_base(base_rid)
            #Meta data
            base_indirection = self.bufferpool.get_record(self.name, INDIRECTION_COLUMN, multipage_id, page_id, record_id, 'Base_Page')
            base_indirection = int.from_bytes(bytes(base_indirection), byteorder='big')
            base_encoding = self.bufferpool.get_record(self.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_id, record_id, 'Base_Page')
            base_encoding = int.from_bytes(base_encoding, byteorder='big')
            #values
            if self.schema_update_check(base_encoding, column_number): #updates
                tail_rid = self.rid_index.locate(base_rid)[-1]
                page_range, record_index = self.rid_tail(tail_rid)
                val = self.bufferpool.get_tail_record(self.name, DEFAULT_COLUMN + column_number, page_range, record_index, 'Tail_Page') 
            else:
                val = self.bufferpool.get_record(self.name, DEFAULT_COLUMN + column_number, multipage_id, page_id, record_id, 'Base_Page')
                val = int.from_bytes(val, byteorder='big')
            #update index
            self.index.update_value(column_number, base_rid, val)

    def close(self):
        self.closed = True
        self.mergetrigger(merge_trigger=True)

    def create_primary_key_index(self):
        self.index.indices[self.key] = BPlusTree(150)
        
        rid_counter = 0
        for key in self.key_lst:
            self.index.create_value(self.key, key, rid_counter)
            rid_counter += 1


    