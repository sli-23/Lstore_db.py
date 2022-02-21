import os
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page

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

class Bufferpool():
    
    def __int__(self):
        self.path = ""
        self.bufferpool = OrderedDict() #key = tablename; val = bufferframe






