from .index import Index
from .config import *
from time import time

#INDIRECTION_COLUMN = 0
#RID_COLUMN = 1
#TIMESTAMP_COLUMN = 2
#SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        
        #Base pages and Tail pages
        """
        Page indirection: # Record - (Page Range)(Base Range)(Bytes)
        example: R3 - (PR)(BP)(16)

        Data modelling:
       | RID | COL | COL | TIMESTAMP_COLUMN | SCHEMA_ENCODING_COLUMN | INDIRECTION |

        """
        self.page_range = []
        self.base_pages = {}
        self.tail_pages = {}
    
    def __merge(self):
        print("merge is happening")
        pass
 
