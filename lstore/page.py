from lstore.config import *



class Page:

    def __init__(self):
        self.num_records = 0 
        self.data = bytearray(4096)
        #bufferpool
        self.dirty = False
        self.pinned = 0

    def has_capacity(self):                 
        return self.num_records < RECORDS_PER_PAGE    # When num_record == 512,the page is full.

    def write(self, value): # sets the an empty array index to value
        self.pinned = 1
        byte_value = value.to_bytes(8, byteorder='big')   # Convert int data to byte data.
        self.data[self.num_records * 8: (self.num_records + 1) * 8] = byte_value  # Write into page.data.
        self.pinned = 0
        self.num_records += 1


    def get(self,index):
        """
        Return byte data by index(The No.index data of page.data).
        :param index: int, the index of data of page.data
        :return: byte, such as b'\x00\x00\x00\x00\x00\x00\x00\x0f' (for int 15)
        """
        self.pinned = 1
        data = self.data[index*8:(index+1)*8]
        self.pinned = 0
        return data

    def updata(self, index, value):
        """
        Update byte data by index to value(Convert to byte)
        :param index: int
        :param value: int
        :return: None
        """
        self.dirty = True
        self.pinned = 1
        self.data[index*8:(index+1)*8] = value.to_bytes(8, byteorder='big')
        self.pinned = 0

    def get_data_num(self):
        return self.num_records


class MultiPage():
    def __init__(self):
        self.index = 0   # Default page index = 0
        self.pages = [Page() for _ in range(MAXPAGE)]
        self.count = 0

    def get_current(self):
        return self.pages[self.index]

    def add_page_index(self):
        self.index += 1

    def last_page(self):
        return self.index == MAXPAGE - 1

    def get_page(self, key):
        if 0 <= key <= self.index:
            return self.pages[key]
     