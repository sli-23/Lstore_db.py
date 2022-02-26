import pickle
from datetime import datetime
import os
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
        self.lru_cache = OrderedDict() #Key: buffer_id = (table_name, column_id, multipage_id, page_range_id, page_id) Value: Page()

    def initial_path(self, path):
        self.path = path

    def check_capacity(self):
        return len(self.lru_cache) >= self.capacity

    def check_page_in_buffer(self, page_id):
        return self.lru_cache[page_id]

    def buffer_to_path(self, table_name, column_id, multipage_id, page_range_id, base_or_tail):
        path = os.path.join(self.path, table_name, base_or_tail, str(column_id) + str(multipage_id)+ str(page_range_id))
        return path

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

    # if add_page, the page is dirty
    def add_page(self, buffer_id, default = True):
        if default:
            self.lru_cache[buffer_id] = None
            self.lru_cache[buffer_id] = None
        else:
            self.lru_cache[buffer_id] = Page()
            self.lru_cache[buffer_id].dirty = True

    def remove_page(self):
        sotred_bufferid = sorted(self.least_used, key = self.least_used.get)
        oldest_budder_id = sotred_bufferid[0] #page

        int = 0
        if self.lru_cache[oldest_budder_id].pinned != 0:
            int += 1
            oldest_budder_id = sotred_bufferid[int]

        oldest_page = self.lru_cache[oldest_budder_id]
        if oldest_page.dirty == 1:
            old_page_path = self.buffer_to_path
            self.write_page(oldest_page, old_page_path)
        
        self.lru_cache[oldest_budder_id] = None
        del self.least_used[oldest_budder_id]


    def get_page(self, table_name, column_id, multipage_id, page_range_id, base_or_tail):
        
        buffer_id = (table_name, column_id, multipage_id, page_range_id, base_or_tail)
        path = self.buffer_to_path(table_name, column_id, multipage_id, page_range_id, base_or_tail)

        #new page
        if not os.path.isfile(path):
            if self.check_capacity():
                self.remove_page()
            self.add_page(buffer_id, default= False)
            
            dirname = os.path.dirname(path)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            f = open(path, 'w+')
            f.close()
        else:
            # page in disk
            if not self.check_page_in_buffer:
                if self.check_capacity(): #if it is full, then remove
                    self.remove_page()
                self.lru_cache[buffer_id] = self.read_page(path)

        return self.lru_cache[buffer_id]

    def get_record(self, table_name, column_id, multipage_id, page_range_id, record_id, base_or_tail):
        page = self.get_page(table_name, column_id, multipage_id, page_range_id, base_or_tail)      
        record_data = page.get(record_id)
        return record_data 



    





