from lstore.index import Index
from lstore.config import *
from lstore.page import Page, MultiPage
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None

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
        self.num_records = 0
        self.page_directory = {}
        self.index = Index(self)
        self.__init_page_directory()

    def __init_page_directory(self):
        """
        Give a default page/multipage obj to page_directory
        :return:
        """
        
        """
        Page indirection: # Record - (Page Range)(Base Range)(Bytes)
        example: R3 - (PR)(BP)(16)

        Data modelling:
       | RID | COL0 | COL1 | TIMESTAMP_COLUMN | SCHEMA_ENCODING_COLUMN | INDIRECTION |

        """
        self.page_directory = {'base':[],'tail':[]}
        for i in range(self.num_columns + DEFAULT_COLUMN):
            self.page_directory['base']=[[MultiPage()] for _ in range(self.num_columns + DEFAULT_COLUMN)]
            self.page_directory['tail'] = [[[Page()]] for _ in range(self.num_columns + DEFAULT_COLUMN)]

    
    def __merge(self):
        print("merge is happening")
        pass

    def get_tail(self, indirection, column, page_index):
        indirection_int = int(str(indirection.decode()).split('\x00')[-1])    # Covert byte to int
        return int.from_bytes(self.page_directory["Tail"][column + DEFAULT_COLUMN][page_index][indirection_int // DEFAULT_COLUMN].get(indirection_int % RECORDS_PER_PAGE), byteorder='big')

    def get_tail_columns(self, indirection, page_index):
        columns = []
        indirection_int = int(str(indirection.decode()).split('\x00')[-1])  # Covert byte to int
        for i in range(0, self.num_columns):
            columns.append(int.from_bytes(self.page_directory["tail"][i+DEFAULT_COLUMN][page_index][indirection_int//RECORDS_PER_PAGE].get(indirection_int%RECORDS_PER_PAGE), byteorder='big'))
        return columns


    def base_write(self, data):
        for i, value in enumerate(data):
            multiPages = self.page_directory["base"][i][-1]
            page = multiPages.get_current()
            if not multiPages.last_page():
                if not page.has_capacity():
                    self.page_directory['base'][i][-1].add_page_index()
                    page = multiPages.get_current()
            else:
                if not page.has_capacity():
                    self.page_directory['base'][i].append(MultiPage())
                    self.page_directory['tail'][i].append([Page()])
                    page = self.page_directory['base'][i][-1].get_current()
            page.write(value)

    def tail_write(self, data, page_index):
        for i, value in enumerate(data):
            if not self.page_directory['Tail'][i][page_index][-1].has_capacity():
                self.page_directory['Tail'][i][page_index].append(Page())
            self.page_directory['Tail'][i][page_index][-1].write(value)

# a = Table('test',5,5)