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
        self.num_updates = 0
        self.updates = []
        self.page_directory = {}
        self.index = Index(self)
        self.key_lst = []
        self.__init_page_directory()

    def __init_page_directory(self):
        """
        Give a default page/multipage obj to page_directory
        :return:

        page directory should map RID to page range, page....

        """

        self.page_directory = {'base':[],'tail':[]}
        for i in range(self.num_columns + DEFAULT_COLUMN):
            self.page_directory['base']=[[MultiPage()] for _ in range(self.num_columns + DEFAULT_COLUMN)]
            self.page_directory['tail'] = [[Page()] for _ in range(self.num_columns + DEFAULT_COLUMN)]

    
    def __merge(self):
        print("merge is happening")
        pass

    def get_tail_indirection(self, indirection, column, page_index):
        indirection_int = int(str(indirection.decode()).split('\x00')[-1])    # Covert byte to int
        return int.from_bytes(self.page_directory["Tail"][column + DEFAULT_COLUMN][page_index][indirection_int // DEFAULT_COLUMN].get(indirection_int % RECORDS_PER_PAGE), byteorder='big')

    def get_tail_columns(self, indirection, page_index):
        columns = []
        indirection_int = int(str(indirection.decode()).split('\x00')[-1])  # Covert byte to int
        for i in range(0, self.num_columns):
            columns.append(int.from_bytes(self.page_directory["tail"][i+DEFAULT_COLUMN][page_index][indirection_int//RECORDS_PER_PAGE].get(indirection_int%RECORDS_PER_PAGE), byteorder='big'))
        return columns


    def base_write(self, data):
        self.num_records += 1
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
                    #self.page_directory['tail'][i].append([Page()])
                    page = self.page_directory['base'][i][-1].get_current()
            page.write(value)


    def tail_write(self, data):
        for i, value in enumerate(data):
            if not self.page_directory['tail'][i][-1].has_capacity():
                self.page_directory['tail'][i].append(Page())
            self.page_directory['tail'][i][-1].write(value)


    def get_base_page_range(self):
        return len(self.page_directory['base'][0])
    
   # return the page range and index for a rid
    def get_record_info(self, rid): #using rid to find record's page_range and index in this page_range's page
        # record: page_range, page_index
        rid_multipage = self.page_directory['base'][RID_COLUMN] #get rid column
        multipage_range = round(rid // (RECORDS_PER_PAGE * MAXPAGE)) # in which multipage
        page_range = round(rid // RECORDS_PER_PAGE) # in which page_range in that multipage
        rid_page = rid_multipage[multipage_range].pages[page_range] # in which page
        
        record_page_range = page_range
        record_page_index = 0

        rid_bytes = rid.to_bytes(8, byteorder='big')
        for i in range(rid_page.num_records):
            if rid_page.get(i) == rid_bytes:
                record_page_index = i

        return record_page_range, record_page_index

    def get_base_rid(self, multipage_range, page_range, record_index): 
        rid_page = self.page_directory['base'][RID_COLUMN][multipage_range].pages[page_range]
        rid = rid_page.get(record_index)
        rid = int.from_bytes(bytes(rid), byteorder='big')
        return rid


    def get_base(self, key):
        key = key.to_bytes(8, byteorder='big')
        page = self.page_directory['base'][DEFAULT_COLUMN + self.key]
        record_index = 0
        record_page_range = 0
        record_multipage = 0
        for i in range(len(page)): # how many multipages
            for j in range(len(page[i].pages)): # page_range in multipage
                for z in range(page[i].pages[j].num_records): #page's number's records
                    if page[i].pages[j].get(z) == key:
                        record_index = z
                        record_page_range = j
                        record_multipage = i
        return record_multipage, record_page_range, record_index

    def get_base_columns(self, rid):
        pass

    def get_tail_key(self, key):
        key = key.to_bytes(8, byteorder='big')
        page = self.page_directory['tail'][DEFAULT_COLUMN + self.key]
        record_page_range = 0
        record_index = 0
        for i in range(len(page)):
            for j in range(page[i].num_records):
                if page[i].get(j) == key:
                    record_page_range = i
                    record_index = j
        return record_page_range, record_index

    def base_ind_tail_rid(self, indirection):
        indirection = indirection.to_bytes(8, byteorder='big')
        page = self.page_directory['tail'][RID_COLUMN]
        record_page_range = 0
        record_index = 0
        for i in range(len(page)):
            for j in range(page[i].num_records):
                if page[i].get(j) == indirection:
                    record_page_range = i
                    record_index = j
        return record_page_range, record_index

    def get_tail_rid(self, rid):
        rid = rid.to_bytes(8, byteorder='big')
        page = self.page_directory['tail'][RID_COLUMN]
        record_page_range = 0
        record_index = 0
        for i in range(len(page)):
            for j in range(page[i].num_records):
                if page[i].get(j) == rid:
                    record_page_range = i
                    record_index = j
        return record_page_range, record_index

    def get_tail_columns(self, rid): #table.columns
        record_page_range, record_index = self.get_tail_rid(rid)
        tail_column = []
        for i in range(self.num_columns):
            page = self.page_directory['tail'][DEFAULT_COLUMN + i]
            val = page[record_page_range].get(record_index)
            val = int.from_bytes(bytes(val), byteorder='big')
            tail_column.append(val)
        return tail_column


    def key_indirection(self, key):
        multipage, page_range, index = self.get_base(key)
        page = self.page_directory['base'][INDIRECTION_COLUMN]
        return page[multipage].pages[page_range].get(index) #bytes

    def key_rid(self, key):
        multipage, page_range, index = self.get_base(key)
        page = self.page_directory['base'][RID_COLUMN]
        return page[multipage].pages[page_range].get(index) #bytes


    def get_schema_encoding_base(self, key):
        page = self.page_directory['base'][DEFAULT_COLUMN + self.key]
        record_index = 0
        record_page_range = 0
        record_multipage = 0
        for i in range(len(page)):
            for j in range(len(page[i].pages)):
                for z in range(page[i].pages[j].num_records):
                    if page[i].pages[j].get(z) == key:
                        record_index = z
                        record_page_range = j
                        record_multipage = i
        return self.page_directory['base'][SCHEMA_ENCODING_COLUMN][record_multipage].pages[record_page_range].get(record_index) #bytes

    def delete_rid_base(self, multipage, page_range, record_index):
        page = self.page_directory['base'][RID_COLUMN]
        page[multipage].pages[page_range].data[record_index * 8 : (record_index + 1) * 8] = (0).to_bytes(8, byteorder='big')

    def delete_rid_tail(self, page_range, record_index):
        page = self.page_directory['tail'][RID_COLUMN]
        page[page_range].data[record_index * 8 : (record_index + 1) * 8] = (0).to_bytes(8, byteorder='big')