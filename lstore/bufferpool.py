import os
from lstore.config import *
from collections import OrderedDict
from lstore.page import page

"""
BufferPool:
- LRU replacer
- Disk Manager
- Page: lstore.page - dirty/pinned
- Frame 
"""

class Bufferpool():
    
    def __int__(self):
        self.bufferpool_size = 0
        self.path = ""
        self.pages = []
        self.table = {} #TODO: multithreading in Milestone 3

        

