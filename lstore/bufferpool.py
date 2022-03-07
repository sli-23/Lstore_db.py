from functools import lru_cache
import pickle
from datetime import datetime
import os
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
from threading import Lock
import copy
import time
import sys

class BufferPool:
    
    def __init__(self, capacity=BUFFERPOOL_SIZE):
        self.path = ""
        self.capacity = capacity
        self.lru_cache = OrderedDict()
        self.last_tail_page = {} #
        self.last_rid = {} #'base': rid; 'tail':rid
        #self.get_latch = Lock()
        
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
        #self.get_latch.acquire()
        buffer_id = (table_name, column_id, multipage_id, page_range_id, base_or_tail)
        path = self.buffer_to_path(table_name, column_id, multipage_id, page_range_id, base_or_tail)
        path = path + '.pkl'
        #new page
        if not os.path.isfile(path): #if in this path there is no such file
            if self.check_capacity():
                self.remove_page()
            self.add_page(buffer_id, default= False)
            
            #make a dir; just a file without any data init
            dirname = os.path.dirname(path)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            f = open(path, 'wb')
            f.close()
    
        else: 
            # page is already in disk
            if not self.check_page_in_buffer:
                if self.check_capacity(): #if it is full, then remove
                    self.remove_page()
                self.lru_cache[buffer_id] = self.read_page(path)
            else:
                # we should suppose that the user will never create same_table names...
                pass
        #self.get_latch.release()
        return self.lru_cache[buffer_id]

    def buffer_to_path_tail(self, table_name, column_id, page_range_id, base_or_tail):
        path = os.path.join(self.path, table_name, base_or_tail, str(column_id) + str(page_range_id))
        return path

    def get_tail_page(self, table_name, column_id, page_range_id, base_or_tail):
        #self.get_latch.acquire()
        buffer_id = (table_name, column_id, page_range_id, base_or_tail)
        path = self.buffer_to_path_tail(table_name, column_id, page_range_id, base_or_tail)
        path = path + '.pkl'
        #new page
        if not os.path.isfile(path): #if in this path there is no such file
            if self.check_capacity():
                self.remove_page()
            self.add_page(buffer_id, default= False)
            
            #make a dir; just a file without any data init
            dirname = os.path.dirname(path)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            f = open(path, 'wb')
            f.close()
    
        else: 
            # page is already in disk
            if not self.check_page_in_buffer:
                if self.check_capacity(): #if it is full, then remove
                    self.remove_page()
                self.lru_cache[buffer_id] = self.read_page(path)
            else:
                # we should suppose that the user will never create same_table names...
                pass
        #self.get_latch.release()
        return self.lru_cache[buffer_id]

    def get_record(self, table_name, column_id, multipage_id, page_range_id, record_id, base_or_tail):
        page = self.get_page(table_name, column_id, multipage_id, page_range_id, base_or_tail)      
        record_data = page.get(record_id)
        return record_data 

    def get_tail_record(self, table_name, column_id, page_range_id, record_id, base_or_tail):
        page = self.get_tail_page(table_name, column_id, page_range_id, base_or_tail) 
        record_data = page.get(record_id)
        return record_data

    def set_new_rid(self, page_type,last_rid): #use it when insert record
        self.last_rid[page_type] = last_rid
    
    def get_last_rid(self, page_type): #get last rid from bufferpool
        if len(self.lru_cache.keys()) == 0: #the bufferpool is empty, no insert or update
            return 0
        
    def last_rid_page(self, page_type):
        if page_type == 'base':
            last_rid = self.last_rid['base']
        else:
            last_rid = self.last_rid['tail']

        last_rid





