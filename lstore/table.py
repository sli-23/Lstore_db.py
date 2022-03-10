from concurrent.futures import thread
from heapq import merge
from lstore.bufferpool import BufferPool
from lstore.index import Index, Tail_Index, Indirection_Index
from lstore.config import *
from lstore.page import Page, MultiPage
from time import time
import threading
from collections import deque, defaultdict
from copy import copy
from lstore.transaction import LockManager
from lstore.Page_Lock import PageLocks, RidLocks
from random import choice, randint, sample, seed

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
        self.updates = []
        self.page_directory = {}
        self.index = Index(self)
        self.tail_index = Tail_Index(self)
        self.indirection_index = Indirection_Index(self)
        self.key_lst = []
        self.last_merge_tail_rid = 0
        self.bufferpool = BufferPool()
        self.mergeQ = deque()
        self.closed = False
        self.page_locks = defaultdict()
        self.__init_page_directory()
        self.lock_manager = LockManager()
        #page lock + rid lock:
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

    def set_table_closed(self):
        self.table_closed = True
        self.merge_trigger.set()
        return
        
    # merge function        
    # https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System
    # check page 6 for details

    def mergetrigger(self, merge_trigger = False):
        if self.num_updates % MERGE_TRIGGER == 0:
            merge_thread = threading.Thread(target=self.merge())
            merge_thread.start()
            merge_thread.join()
        elif merge_trigger == True:
            merge_thread = threading.Thread(target=self.merge())
            merge_thread.start()
            merge_thread.join()
    
    def get_tail_range(self):
        # get current tail range from mergeQ
        start_rid = self.mergeQ[0]
        end_rid = self.mergeQ[-1]
        start_range, record_id = self.rid_tail(start_rid)
        end_range, record_id = self.rid_tail(end_rid)
        return start_range, end_range + 1


    def merge_check_tool(self, multipage, page_range, record_index):
        base_page = self.page_directory['base']
        column = []
        for i in range(len(base_page)):
            val = self.bufferpool.get_record('Grades', i, multipage, page_range, record_index, 'Base_Page')
            val = int.from_bytes(val, byteorder='big')
            column.append(val)
        return column


    def merge(self):
        print('Starting merging....')
        # Using mergeQ to calculate the tail page range
        tail_start_range, tail_end_range = self.get_tail_range()

        for column_id in range(4, 4 + self.num_columns):
            #get the base page range in that tail merge range
            base_page_range = self.get_merged_base_range(column_id)
            base_page_range_copy = copy(base_page_range)
            print(base_page_range_copy)
            update = {}
            for buffer_id in base_page_range_copy.keys():
                table_name, column_id, multipage_id, page_range_id, base_or_tail = buffer_id
                print(buffer_id)
                for record_id in range(int(RECORDS_PER_PAGE)):
                    update[(table_name, column_id, multipage_id, page_range_id, record_id, base_or_tail)] = 0
            
            for tail_merge_range in reversed(range(tail_start_range, tail_end_range)):
                for reversed_record in reversed(range(int(RECORDS_PER_PAGE))):
                    # Step 0: get the tail indirection 
                    tail_indirection = self.bufferpool.get_tail_record(self.name, INDIRECTION_COLUMN, tail_merge_range, reversed_record, 'Tail_Page')
                    tail_indirection = int.from_bytes(tail_indirection, byteorder='big')
                    base_rid = self.indirection_index.locate(tail_indirection)[0][1]
                    
                    primary_key = self.indirection_index.locate(tail_indirection)[0][0]
                    lastest_indirection = self.tail_index.locate(primary_key)[0][0]
                    # Step a: using the base rid to get multipage_index, page_range_index, record_index
                    multipage_index, page_range_index, record_index = self.rid_base(base_rid)

                    if tail_indirection != lastest_indirection:
                        continue
                    else:
                        # Step b: write buffer_id_base (using it to map page in dict), buffer_id_base_record (update dict to check update)
                        buffer_id_base = (self.name, column_id,multipage_index, page_range_index, 'Base_Page')
                        buffer_id_base_record = (self.name, column_id, multipage_index, page_range_index, record_index, 'Base_Page')
                        
                        # Step c: get tail value in specific column, page range, and record index
                        updated_val = self.bufferpool.get_tail_record(self.name, column_id, tail_merge_range, reversed_record, 'Tail_Page')
                        updated_val = int.from_bytes(updated_val, byteorder='big')
                        if update[buffer_id_base_record] == 0:
                            if updated_val != MAXINT: #update
                                page = base_page_range_copy[buffer_id_base]
                                page.update(record_index, updated_val)
                                old_schema = self.bufferpool.get_page(self.name ,SCHEMA_ENCODING_COLUMN, multipage_index, page_range_index, 'Base_Page').get(record_index)
                                old_schema = int.from_bytes(old_schema, byteorder='big')
                                new_schema = self.update_schema_encoding(column_id,old_schema)
                                self.bufferpool.get_page(self.name, SCHEMA_ENCODING_COLUMN, multipage_index, page_range_index, 'Base_Page').update(record_index, new_schema)
                        update[buffer_id_base_record] = 1

            self.bufferpool.merge_base_range(base_page_range_copy)
        update = {}
        #clear mergeQ 
        self.mergeQ.clear()
        return
    
    def get_merged_base_page(self):
        rid_start = self.mergeQ[0]
        rid_end = self.mergeQ[-1]

        start_page_range, start_record_index = self.rid_tail(rid_start)
        end_page_range, end_record_index = self.rid_tail(rid_end)

        indirection_start = self.bufferpool.get_tail_record(self.name, INDIRECTION_COLUMN, start_page_range, start_record_index, 'Tail_Page')
        indirection_start = int.from_bytes(indirection_start, byteorder='big')
        indirection_end = self.bufferpool.get_tail_record(self.name, INDIRECTION_COLUMN, end_page_range, end_record_index, 'Tail_Page')
        indirection_end = int.from_bytes(indirection_end, byteorder='big')

        start_base_rid = self.indirection_index.locate(indirection_start)[0][1]
        end_base_rid = self.indirection_index.locate(indirection_end)[0][1]

        start_multipage_id, start_page_range_id, start_record_id = self.rid_base(start_base_rid)
        end_multipage_id, end_page_range_id, end_record_id = self.rid_base(end_base_rid)

        return ([start_multipage_id, start_page_range_id], [end_multipage_id, end_page_range_id])

    #then using above information to get page range in a dict

    def get_merged_base_range(self, column_id):
        page_range = {}
        merged_base = self.get_merged_base_page() #tuple
        start_base = merged_base[0]
        end_base = merged_base[1]

        # if their multipage is the same
        if start_base[0] == end_base[0]:
            for page_id in range(start_base[1], end_base[1] + 1):
                buffer_id = (self.name, column_id, start_base[0], page_id, 'Base_Page')
                page = self.bufferpool.get_page(self.name, column_id, start_base[0], page_id, 'Base_Page')
                page_range[buffer_id] = page
        else: # different multipage
            #start:
            for page_id in range(start_base[1], MAXPAGE):
                buffer_id = (self.name, column_id, start_base[0], page_id, 'Base_Page')
                page = self.bufferpool.get_page(self.name, column_id, start_base[0], page_id, 'Base_Page')
                page_range[buffer_id] = page
            #end
            for page_id in range(0, end_base[1] + 1):
                buffer_id = (self.name, column_id, start_base[0], page_id, 'Base_Page')
                page = self.bufferpool.get_page(self.name, column_id, start_base[0], page_id, 'Base_Page')
                page_range[buffer_id] = page

        return page_range

    def update_schema_encoding(self, column_index, old_schema):
        old_schema = bin(old_schema)[2:].zfill(self.num_columns)
        new_schema = int(old_schema[:self.num_columns-(column_index - DEFAULT_COLUMN)-1] + "0" + old_schema[self.num_columns-(column_index - DEFAULT_COLUMN):], 2)
        return new_schema

    def base_write(self, data):
        for i, value in enumerate(data):
            #calculate rid to get multipage, page_range, index
            #rid = number of records
            multipage_id, page_range_id, record_id = self.rid_base(self.num_records)
            multiPages = self.page_directory["base"][i][-1]
            page1 = multiPages.get_current()
            #write the page in bufferpool
            page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            if not multiPages.last_page(): #not the last page
                if not page1.has_capacity() and not page2.has_capacity(): #page is full
                    self.page_directory['base'][i][-1].add_page_index()
                    page_range_id += 1
                    page1 = multiPages.get_current()
                    page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            else: #is the last page
                if not page1.has_capacity() and not page2.has_capacity(): #if the last page in the multipage is full then
                    self.page_directory['base'][i].append(MultiPage())
                    multipage_id += 1
                    page_range_id = 0
                    page1 = self.page_directory['base'][i][-1].get_current()
                    page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')

            page1.write(value)
            page2.write(value)
            page1.dirty = True
            page2.dirty = True

    def tail_write(self, data):
        for col, val in enumerate(data):
            page_id, record_index = self.rid_tail(self.num_updates)
            page = self.bufferpool.get_tail_page(self.name, col, page_id, 'Tail_Page') #write in butter
            tail_page = self.page_directory['tail'][col][-1] # write in tail page for test only
            if not tail_page.has_capacity() and not page.has_capacity():
                self.page_directory['tail'][col].append(Page())
                tail_page = self.page_directory['tail'][col][-1]
                page_id += 1
                page = self.bufferpool.get_tail_page(self.name, col, page_id, 'Tail_Page')
            
            page.write(val)
            tail_page.write(val)
            tail_page.dirty = True
            page.dirty = True
            
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
            multipage_id, page_id, record_id = self.table.rid_base(base_rid)
            #Meta data
            base_indirection = self.bufferpool.get_record(self.name, INDIRECTION_COLUMN, multipage_id, page_id, record_id, 'Base_Page')
            base_indirection = int.from_bytes(bytes(base_indirection), byteorder='big')
            base_encoding = self.bufferpool.get_record(self.name, SCHEMA_ENCODING_COLUMN, multipage_id, page_id, record_id, 'Base_Page')
            base_encoding = int.from_bytes(base_encoding, byteorder='big')
            #values
            if self.schema_update_check(base_encoding, column_number): #updates
                tail_rid = self.indirection_index.locate(base_indirection)[0][2]
                page_range, record_index = self.rid_tail(tail_rid)
                val = self.bufferpool.get_tail_record(self.name, DEFAULT_COLUMN + column_number, page_range, record_index, 'Tail_Page') 
            else:
                val = self.bufferpool.get_record(self.name, DEFAULT_COLUMN + column_number, multipage_id, page_id, record_id, 'Base_Page')
                val = int.from_bytes(val, byteorder='big')
            #update index
            self.index.update_index(column_number, base_rid, val)

    def close(self):
        self.closed = True
        self.mergetrigger(merge_trigger=True)

    