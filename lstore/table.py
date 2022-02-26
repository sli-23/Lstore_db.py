import enum
from lstore.bufferpool import BufferPool
from lstore.index import Index
from lstore.config import *
from lstore.page import Page, MultiPage
from time import time
import threading
from collections import deque

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
        self.key_lst = []

        self.bufferpool = BufferPool()

        """
        Each base page has a merge queue
        merge queue is a deque 
        with element (BaseRid, TailRid) for every update
        """
        self.__init_page_directory()

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

    
    def merge(self):
        print("Starting merging")
        #Get Base RID, base indirection
        base_rid = self.index.locate(self.table.key, )[0]
        multipage_range, page_range, record_index = self.rid_base(base_rid)
        base_indirection = self.table.page_directory['base'][INDIRECTION_COLUMN][multipage_range].pages[page_range].get(record_index)
        base_indirection_int = int.from_bytes(bytes(base_indirection), byteorder='big')
     

    def get_tail_indirection(self, indirection, column, page_index):
        indirection_int = int(str(indirection.decode()).split('\x00')[-1])    # Covert byte to int
        return int.from_bytes(self.page_directory["Tail"][column + DEFAULT_COLUMN][page_index][indirection_int // DEFAULT_COLUMN].get(indirection_int % RECORDS_PER_PAGE), byteorder='big')

    def get_tail_columns(self, indirection, page_index):
        columns = []
        indirection_int = int(str(indirection.decode()).split('\x00')[-1])  # Covert byte to int
        for i in range(0, self.num_columns):
            columns.append(int.from_bytes(self.page_directory["tail"][i+DEFAULT_COLUMN][page_index][indirection_int//RECORDS_PER_PAGE].get(indirection_int%RECORDS_PER_PAGE), byteorder='big'))
        return columns

    def base_write(self, data):
        for i, value in enumerate(data):
            #calculate rid to get multipage, page_range, index
            #rid = number of records
            multipage_id, page_range_id, page_id = self.rid_base(self.num_records)
            
            multiPages = self.page_directory["base"][i][-1]
            page = multiPages.get_current()
            page = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            if not multiPages.last_page(): #not the last pag
                if not page.has_capacity(): #page is full 
                    self.page_directory['base'][i][-1].add_page_index()
                    page_range_id += 1
                    page = multiPages.get_current()
                    print(self.name, i, multipage_id, page_range_id)
                    page = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            else:
                if not page.has_capacity():
                    self.page_directory['base'][i].append(MultiPage())
                    multipage_id += 1
                    page_range_id = 0
                    page = self.page_directory['base'][i][-1].get_current()
                    page = self.bufferpool.get_page(self.name, i, multipage_id, page_range_id, 'Base_Page')
            
            page.dirty = True
            page.write(value)


    def tail_write(self, data):
        for col, val in enumerate(data):
            page_id, record_index = self.rid_tail(self.num_updates)
            page = self.bufferpool.get_page(self.name, col, page_id, page_id, 'Tail_Page')
            if val == None:
                continue
            else:
                tail_page = self.page_directory['tail'][col][-1]
                if not tail_page.has_capacity():
                    self.page_directory['tail'][col].append(Page())
                    tail_page = self.page_directory['tail'][col][-1]
                    page_id += 1
                    page = self.bufferpool.get_page(self.name, col, page_id, page_id, 'Tail_Page')
                
                tail_page.dirty = True
                tail_page.write(val)

    def rid_base(self, rid):
        record_multipage = 0
        if rid % (RECORDS_PER_PAGE * MAXPAGE) == 0:
            record_multipage = rid / (RECORDS_PER_PAGE * MAXPAGE) - 1
        else:
            record_multipage = rid // (RECORDS_PER_PAGE * MAXPAGE)

        if record_multipage < 0:
            record_multipage = 0
        
        rid = rid - RECORDS_PER_PAGE * MAXPAGE * record_multipage
        
        record_page_range = 0
        if rid % RECORDS_PER_PAGE == 0:
            record_page_range = (rid / RECORDS_PER_PAGE) - 1
        else:
            record_page_range = (rid // RECORDS_PER_PAGE)

        if record_page_range < 0:
            record_page_range = 0

        rid = rid - (RECORDS_PER_PAGE * record_page_range)

        record_index = rid - 1
        if record_index < 0:
            record_index = 0

        return int(record_multipage), int(record_page_range), int(record_index)

    def rid_tail(self, rid):
        page_range = 0
        record_index = 0

        if rid // RECORDS_PER_PAGE != 1:
            page_range = rid // RECORDS_PER_PAGE
        else:
            page_range = rid // RECORDS_PER_PAGE - 1 #if update = 512 => is full but still in page_range 1
        
        rid = rid - RECORDS_PER_PAGE * page_range

        record_index = rid - 1
        if record_index < 0:
            record_index = 0

        return int(page_range), int(record_index)

    def get_schema_encoding(self, indirection):
        #convert into string
        pass

    #TODO: bufferpool update
    def get_recent_tail_record(self, base_indirection):
        tail_page = self.page_directory['tail'][INDIRECTION_COLUMN] #list with page()

        pass

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

    def merge(self, primary_key):
        #get rid
        base_rid =  self.index.locate(self.table.key, primary_key)[0]
        (multipage_range, page_range, record_index) = self.table.rid_base(base_rid)
        