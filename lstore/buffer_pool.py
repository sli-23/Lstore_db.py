import os
from tkinter import Frame
from typing_extensions import Self
from lstore.config import *
from collections import OrderedDict
from lstore.page import Page
import threading
import time
import sys


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