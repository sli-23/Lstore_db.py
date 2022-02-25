import os
from tkinter import Frame
from typing_extensions import Self
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
import threading
import time
import sys

"""
BufferPool:
- LRU replacer
- Disk Manager
- Page: lstore.page - dirty/pinned
- Frame 
"""

"""
Update value:
1. pinnedcount += 1
2. update page
3. dirty = True
4. pinnedcount -= 1

Write value:
1. pinnedcount += 1
2. write page
3. dirty = True
4, pinnedcount -= 1

get value (with index):
1. pinnedcount += 1
2. get()
3. pinnedcount -= 1
"""

class Bufferpool_Frame:

    def __init__(self, table_name, num_columns, page_num):
        self.page_number = page_num
        self.table = table_name
        self.dirtybit = False
        self.pincount = 0
        self.reference_counter = 0 #each page has a reference bit; when a page is accessed, set to 1
        self.transactions = 0

    def pin_page(self):
        self.transactions -= 1

    def unpin_page(self):
        self.transactions += 1

    def dirty(self):
        self.dirtybit = True
    
    def clean(self):
        self.dirtybit = False
        

class Bufferpool:
    
    def __int__(self, capacity):
        self.path = ""
        self.capacity = capacity
        #self.bufferpool = OrderedDict() #key = tablename; val = bufferframe
        self.lru_cache = OrderedDict() #(,frame)
        self.bufferpool_lock = threading.Lock()
        last_tail = {} #(table_name, col, multipage, page_range) value: lastest tail
        self.page_num = 0
        self.table_directory = {}

    def initial_path(self, path):
        self.path = path

    def read_from_disk(table_name, num_columns, page_num):
        # fetch pages from disk
        seek_offest = int(page_num / num_columns)
        seek_bytes = PAGE_SIZE
        file_num = page_num % num_columns
        file_name = Self.path + '/' + table_name + '_' + str(file_num) + '.pkg'

        if not os.path.exists(file_name):
            return None
        else:
            page = Page(page_num)
            f = open(file_name, 'rb')
            f.seek(seek_offest * seek_bytes)
            data = f.read(seek_bytes)
            
            frame = Frame(table_name, num_columns, page_num)
            return frame

    def add_new_page(self, table_name, num_columns, page_num):
        page = Page(page_num)
        self.page_num += 1

    def evict(self, recent_used=False):
        #check the outstanding transaction of the least recently used from
        lru_frame = self.lru_cache.popitem(last=recent_used)
        self.page_num -= 1
        key = lru_frame.key
        dirty = lru_frame.dirtybit #frame function

        if dirty:
            lru_frame.write_frame(self.path)


    def read_frame(self, table_name, num_columns, page_num):
        if page_num not in self.lru_cache:
            if self.page_num >= BUFFERPOOL_SIZE:
                #evict
                self.page_num += 1
            frame = self.read_from_disk(table_name, num_columns, page_num)
            if frame is None:
                frame = self.add_new_page(table_name, num_columns)
        else:
            self.lru_cache.move_to_end(page_num)
            return self.lru_cache[page_num]

    def add_page(self, able_name, page_number, page_id, page):
        pass