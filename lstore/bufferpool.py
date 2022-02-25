import pickle
from datetime import datetime
import os
from tkinter import Frame
from typing_extensions import Self
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
import threading
import copy
import time
import sys

class BufferPool:
    
    def __init__(self, capacity=BUFFERPOOL_SIZE):
        self.path = ""
        self.capacity = capacity
        self.page_num = 0
        self.lru_cache = OrderedDict() #key: bufferid = (table_name, column_id, multipage_id, page_range_id, page_id) value: Page()
        self.page_bufferpool = {}
        self.least_used = {}
        self.tail = {}

    def initial_path(self, path):
        self.path = path

    def check_capacity(self):
        return len(self.page_bufferpool) >= self.capacity

    def check_page_in_buffer(self, page_id):
        return self.page_bufferpool[page_id]

    def buffer_to_path(self, table_name, column_id, multipage_id, page_range_id, page_id):
        path = os.path.join(self.path, table_name, str(column_id) + str(multipage_id)+ str(page_range_id) + str(page_id) + '.pkg')
        return path
    
    def print(self):
        print(self.page_bufferpool)

    def read_page(self, path):
        f = open(path, 'rb')
        page = pickle.load(f)
        buffer_page = Page()
        buffer_page.num_records = page.num_records
        buffer_page.data = page.data
        f.close
        return buffer_page


    def write_page(self, page, path):
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        f = open(path, 'wb')
        pickle.dump(page, f)
        f.close()

    def add_page(self, buffer_id, default = True):
        if default:
            self.page_bufferpool[buffer_id] = None
            self.lru_cache[buffer_id] = None
        else:
            self.page_bufferpool[buffer_id] = Page()
            self.lru_cache[buffer_id] = Page()
            self.page_bufferpool[buffer_id].dirty = True

    def remove_page(self):
        sotred_bufferid = sorted(self.least_used, key = self.least_used.get)
        oldest_budder_id = sotred_bufferid[0] #page

        int = 0
        if self.page_bufferpool[oldest_budder_id].pinned != 0:
            int += 1
            oldest_budder_id = sotred_bufferid[int]

        oldest_page = self.page_bufferpool[oldest_budder_id]
        if oldest_page.dirty == 1:
            old_page_path = self.buffer_to_path
            self.write_page(oldest_page, old_page_path)
        
        self.page_bufferpool[oldest_budder_id] = None
        del self.least_used[oldest_budder_id]

    #需要改
    def get_page(self, table_name, column_id, multipage_id, page_range_id, page_id):
        buffer_id = (table_name, column_id, multipage_id, page_range_id, page_id)
        path = self.buffer_to_path(table_name, column_id, multipage_id, page_range_id, page_id)

        #if not file
        if not os.path.isfile(path):
            if self.check_capacity(): #if it is full
                self.remove_page()
            self.add_page(buffer_id, default= False)
            dirname = os.path.dirname(path)
            
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            f = open(path, 'w+')
            f.close()
        else:
            #has existed page
            if not self.check_page_in_buffer:
                if self.check_capacity(): #if it is full
                    self.remove_page()
                self.page_bufferpool[buffer_id] = self.read_page(path)
        
        #self.least_used[buffer_id] = time()
        
        return self.page_bufferpool[buffer_id]

    def get_record(self, table_name, column_id, multipage_id, page_range_id, page_id, record_id):
        page = self.get_page(table_name, column_id, multipage_id, page_range_id, page_id)      
        record_data = page.get(record_id)
        return record_data  

        

    





