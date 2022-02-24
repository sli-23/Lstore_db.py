import os
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
import threading
import time

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

1. LRU Replacement Policy
    LRU,即最近最久未被使用的页面会被置换出去
    LRU负责追踪Page_数组中,最久未被使用的元素的index(即frame_id)

victim(frame id): 将最近最久未被使用的对象移除,将它的内容存到输入参数 中并返回true,如果Replacer为空,则返回false
pin(frame id): 此方法应该在一个Page被固定到BufferPoolManager的frame后调用,它应该从LRUReplacer中删除该frame
unpin(frame id): 这个方法当一个page的pin_count变成0的时候被调用,这个方法会添加frame,该frame会包含未被固定的page
size(): 这个方法会返回当前LRUReplacer中的frame的数量


2. Buffer Pool Manager Instance
BPMI(Buffer Pool Manager Instance)负责从DiskManager拿数据库的page,并将他们存在内存。它也负责将dirty page 放回磁盘,或者当内存不足时,交换内存Page回磁盘。
BPMI会复用Page对象。意味着,同一个Page对象在系统运行期间,可能包含不同的物理页面。Page对象的标识page_id会标识当前Page对象包含的是哪一个物理页面。
如果一个Page对象不包含任何物理页面,则它的page_id必须被设置成INVALID_PAGE_ID。

每一个Page对象会维持一个计数器,表示当前有多少个线程已经“pinned”该Page。BPMI不会释放掉一个被“Pinned”的Page。每个Page对象同时会实时记录自己是否是dirty。
你的程序要记录一个page在它被"unpinned"前,是否被修改了。BPMI要在Page对象重新被使用之前,将dirty Page写回磁盘。

对于FetchPgImp,如果空闲列表中没有可用的Page,且其他所有的Page都处于”pinned“状态,你需要返回NULL。

对于FlushPgImp,无论Page处于什么状态（“pinned” 或 “unpinned”）,都会被刷新到磁盘。

对于UnpinPgImp,is_dirty参数会告知Page有没有被修改。

注意：Pin和Unpin在LRUReplacer和BufferPoolManagerInstance有不同的含义。在LRUReplacer中，pin一个Page意味着我们不应该将该Page调出去，因为该Page在被使用，所以要将该Page移出LRU的list。在BufferPoolManagerInstance中，pin一个Page，意味着我们想要使用这个Page，这个Page不应该被移出Buffer Pool。


3. Parallel BUffer Pool Manager
"""

class Bufferpool_Frame:

    def __init__(self, table_name, page_number, page):
        self.page_number = page_number
        self.table = table_name
        self.dirtybit = False
        self.pincount = 0
        self.currentpage = page
        self.reference_counter = 0 #each page has a reference bit; when a page is accessed, set to 1
        pass

class DiskManager:

    def __init__(self) -> None:
        pass

class Bufferpool:
    
    def __int__(self, capacity):
        self.path = ""
        self.capacity = capacity
        #self.bufferpool = OrderedDict() #key = tablename; val = bufferframe
        self.bufferpool = OrderedDict() #(,frame)
        
        self.bufferpool_lock = threading.Lock()

    def __len__(self):
        return self.pages

    def initial_path(self, path):
        self.path = path
    
    def evict(self):
        pass


    





