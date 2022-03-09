import threading
import multiprocessing as m
from collections import defaultdict
from tkinter import E

class PageLocks:
    def __init__(self):
        self.page_locks = defaultdict(self.defaultdict_val)

    def defaultdict_val(self):
        return defaultdict(threading.Lock)

    def acquire_page_lock(self, table_name, column_num, multipage_id, page_range):
        self.page_locks[table_name][(column_num, multipage_id, page_range)].acquire()

    def release_page_lock(self, table_name, column_num, multipage_id, page_range):
        self.page_locks[table_name][(column_num, multipage_id, page_range)].release()

class RidLocks:
    def __init__(self):
        self.rid_locks = defaultdict(self.defaultdict_val)

    def defaultdict_val(self):
        return defaultdict(threading.Lock)

    def acquire_tail_lock(self, table_name, column_num, page_range):
        self.rid_locks[table_name][(column_num, page_range)].acquire()

    def release_tail_lock(self, table_name, column_num, page_range):
        self.rid_locks[table_name][(column_num, page_range)].release()