import pickle
from datetime import datetime
import os
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
from threading import Lock
import copy
import time

class BufferPool:
    
    def __init__(self, capacity=BUFFERPOOL_SIZE):
        self.path = ""
        self.size = BUFFERPOOL_SIZE
        self.capacity = capacity
        self.lru_cache = OrderedDict() #lru
        self.last_tail_page = {} #
        self.last_rid = {} #'base': rid; 'tail':rid
        self.page_directories = {}#read data from file
        
    def initial_path(self, path):
        self.path = path

    def check_capacity(self):
        return len(self.lru_cache) >= self.capacity

    def check_page_in_buffer(self, buffer_id):
        try:
            return self.lru_cache[buffer_id]
        except:
            return False

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
        else:
            self.lru_cache[buffer_id] = Page()
            self.lru_cache[buffer_id].dirty = True

    def get_page(self, table_name, column_id, multipage_id, page_range_id, base_or_tail):
        buffer_id = (table_name, column_id, multipage_id, page_range_id, base_or_tail)
        path = self.buffer_to_path(table_name, column_id, multipage_id, page_range_id, base_or_tail)
        path = path + '.base'
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
            if self.check_page_in_buffer(buffer_id) == False:
                if self.check_capacity(): #if it is full, then remove
                    self.remove_page()
                self.lru_cache[buffer_id] = self.read_page(path)
        return self.lru_cache[buffer_id]

    def buffer_to_path_tail(self, table_name, column_id, page_range_id, base_or_tail):
        path = os.path.join(self.path, table_name, base_or_tail, str(column_id) + str(page_range_id))
        return path

    def get_tail_page(self, table_name, column_id, page_range_id, base_or_tail):
        #self.get_latch.acquire()
        buffer_id = (table_name, column_id, page_range_id, base_or_tail)
        path = self.buffer_to_path_tail(table_name, column_id, page_range_id, base_or_tail)
        path = path + '.tail'
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
            if not self.check_page_in_buffer(buffer_id):
                if self.check_capacity(): #if it is full, then remove
                    self.remove_page()
                self.lru_cache[buffer_id] = self.read_page(path)
        #self.get_latch.release()
        return self.lru_cache[buffer_id]

    def get_record(self, table_name, column_id, multipage_id, page_range_id, record_id, base_or_tail):
        page = self.get_page(table_name, column_id, multipage_id, page_range_id, base_or_tail)      
        record_data = page.get(record_id)
        return record_data

    def get_tail_record(self, table_name, column_id, page_range_id, record_id, base_or_tail):
        buffer_id = (table_name, column_id, page_range_id, base_or_tail)
        page = self.lru_cache[buffer_id]
        record_data = page.get(record_id)
        return record_data

    def set_new_rid(self, page_type,last_rid): #use it when insert record
        self.last_rid[page_type] = last_rid
    
    def get_last_rid(self, page_type): #get last rid from bufferpool
        if len(self.lru_cache.keys()) == 0: #the bufferpool is empty, no insert or update
            return 0

    def buffer_id_path_base(self, buffer_id):
        table_name, column_id, multipage_id, page_range_id, base_or_tail = buffer_id
        path = os.path.join(self.path, table_name, base_or_tail, str(column_id) + str(multipage_id)+ str(page_range_id) + '.base')
        return path

    def buffer_id_path_tail(self, buffer_id):
        table_name, column_id, page_range_id, base_or_tail = buffer_id
        path = os.path.join(self.path, table_name, base_or_tail, str(column_id)+ str(page_range_id) + '.tail')
        return path
    
    def evict(self):
        buffer_id_lst = list(self.lru_cache.keys())
        while len(buffer_id_lst) > 0:
            for buffer_id in buffer_id_lst:
                page = self.lru_cache[buffer_id]
                if page.dirty == True and page.pinned == 0:
                    if buffer_id[-1] == 'Base_Page':
                        path = self.buffer_id_path_base(buffer_id)
                    else:
                        path = self.buffer_id_path_tail(buffer_id)
                    self.write_page(page, path)
                    buffer_id_lst.pop(buffer_id_lst.index(buffer_id))
                if page.dirty == False:
                    buffer_id_lst.pop(buffer_id_lst.index(buffer_id))
            time.sleep(1)

    def remove_page(self):
        print('Start removing pages from BufferPool')
        buffer_id_list = list(self.lru_cache.keys())
        oldest_buffer_id = buffer_id_list[0]

        #check if the page is pinned
        buffer_id_count = 0
        while self.lru_cache[oldest_buffer_id].pinned != 0:
            buffer_id_count += 1
            oldest_buffer_id = buffer_id_list[buffer_id_count]
            if self.lru_cache[oldest_buffer_id].pinned == 0:
                break

        oldest_page = self.lru_cache[oldest_buffer_id]
        assert(oldest_page is not None)
        #check if the page is dirty
        
        if oldest_page.dirty == True:
            if oldest_buffer_id[-1] == 'Base_Page':
                oldest_page_path = self.buffer_id_path_base(oldest_buffer_id)
            else:
                oldest_page_path = self.buffer_id_path_tail(oldest_buffer_id)
            self.write_page(oldest_page, oldest_page_path)

        self.lru_cache[oldest_buffer_id] = None
        self.lru_cache.pop(oldest_buffer_id)
    
    def close(self):
        #clean bufferpool after close the table.
        pass

    def merge_base_range(self, page_range):
        for buffer_id, page in page_range.items():
            self.lru_cache[buffer_id] = page