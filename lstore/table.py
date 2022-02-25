from email.mime import base
import enum

from numpy import delete
from lstore.bufferpool import BufferPool
from lstore.index_bplustree import Index
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
            multiPages = self.page_directory["base"][i][-1]
            page = multiPages.get_current()
            if not multiPages.last_page():
                if not page.has_capacity():
                    self.page_directory['base'][i][-1].add_page_index()
                    page = multiPages.get_current()
            else:
                if not page.has_capacity():
                    self.page_directory['base'][i].append(MultiPage())
                    #self.page_directory['tail'][i].append([Page()])
                    page = self.page_directory['base'][i][-1].get_current()
            page.write(value)


    def tail_write(self, data):
        for col, val in enumerate(data):
            tail_page = self.table.page_directory['tail'][col][-1]
            if not tail_page.has_capacity():
                self.table.page_directory['tail'][col].append(Page())
                tail_page = self.table.page_directory['tail'][col][-1]
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

    def close(self):
        pass