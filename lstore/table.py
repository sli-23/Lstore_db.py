import enum
from lstore.bufferpool import BufferPool
from lstore.index import Index, Tail_Index
from lstore.config import *
from lstore.page import Page, MultiPage
from time import time
import threading
from collections import deque
from copy import copy

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
        self.key_lst = []

        self.bufferpool = BufferPool()
        self.mergeQ = deque()
        #self.merge_queue_matrix = []

        self.closed = False

        """
        Each base page has a merge queue
        merge queue is a deque 
        with element (BaseRid, TailRid) for every update
        """
        self.__init_page_directory()
    
    def __setstate__(self):
        self.merge_trigger = threading.Event()
        self.merge_thread = threading.Thread(name='merge', target=self.merge)
    
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

    def get_merge_range_base(self, tail_merge_range):
        rid = tail_merge_range * RECORDS_PER_PAGE
        multipage_id, page_range, record_index = self.rid_base(rid)
        return multipage_id, page_range

    def get_merge_range_tail(self):
        last_tail_rid = self.bufferpool.last_rid_page('tail')
        page_range, record_index = self.rid_base(last_tail_rid)
        return page_range
    
    # merge function        
    # https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System
    # check page 6 for details

    def mergetrigger(self):
        if self.num_updates % MERGE_TRIGGER == 0:
            self.merge_trigger.set()
            self.merge_thread.start()
            #self.merge_thread.join()
            
    def merge(self):
        print("Start merging")
        while not self.closed: #if the table is not closed
            # step0: wait until all concurrent merge is empty
            self.merge_trigger.wait()
            multipage_id, merge_range = self.get_merge_range()
            merge_range = merge_range + 1
            
            if self.closed:
                 self.merge_range += 1
            
            # create a copy of base range
            for col in range(self.num_columns):
                for page_index in range(self.merge_range):
                    merge_queue = self.merge_queue_matrix[col][page_index]
                    column_index = DEFAULT_COLUMN + col
                    new_base_page = copy(self.bufferpool.get_page(self.name, column_index, multipage_id, page_index, 'base'))
                    seen_update = {}

                    while len(merge_queue) != 0:
                        tuple = merge_queue.popleft()
                        base_rid = tuple[0]
                        tail_rid = tuple[1]
                        seen_update[base_rid] = tail_rid

                    for base_rid, tail_rid in seen_update.items():
                        pass
            self.merge_trigger.clear()
        return

    def base_write(self, data):
        self.num_records += 1
        for i, value in enumerate(data):
            #calculate rid to get multipage, page_range, index
            #rid = number of records
            multipage_id, page_range_id, record_id = self.rid_base(self.num_records)
            multiPages = self.page_directory["base"][i][-1]
            page1 = multiPages.get_current()
            #write the page in bufferpool
            page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            if not multiPages.last_page(): #not the last pag
                if not page1.has_capacity(): #page is full 
                    self.page_directory['base'][i][-1].add_page_index()
                    page_range_id += 1
                    page1 = multiPages.get_current()
                    page2 = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            else:
                if not page1.has_capacity():
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
            if not tail_page.has_capacity():
                self.page_directory['tail'][col].append(Page())
                tail_page = self.page_directory['tail'][col][-1]
                page_id += 1
                page = self.bufferpool.get_tail_page(self.name, col, page_id, 'Tail_Page')
                
            tail_page.dirty = True
            tail_page.write(val)
            page.write(val)

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


    def invalidate_record(self, rid):
        # invalidates record based on its rid
        self.lock.acquire()
        # invalidate base record
        #delete
        self.lock.release()
        pass

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
        return base_indirection - tail_rid - base_rid - 1

    # get data from bufferpool
    def get_tail_record_column(self, tail_rid, column):
        page_range, record_index = self.rid_tail(tail_rid)
        val = self.bufferpool.get_tail_record(self.name, column, page_range, record_index, 'Tail_Page') #bytes
        val = int.from_bytes(val, byteorder='big')
        return val

    # using tail_rid to get record from bufferpool
    def get_tail_record(self, tail_rid):
        column = []
        for i in range(self.num_columns):
            column.append(self.get_tail_record_column(tail_rid, 4 + i))
        return column

    