from lstore.config import *

"""
the maximum record should be 4096 / 8 bytes

 id
----
R1 1  
R2 2 - 8 bytes
R3 3 - 16 bytes
"""

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):                 
        capacity = RECORDS_PER_PAGE - self.num_records
        if capacity == 0:
            return False
        else:
            return True

    def write(self, value):                 # sets the an empty array index to value
        if self.has_capacity():
            byte_value = value.to_bytes(8, byteorder='big')   # Convert int data to byte data.
            self.data[self.num_records * 8: (self.num_records + 1) * 8] = byte_value  # Write into page.data.
            self.num_records += 1
        else:
            raise IndexError('No capacity')

    def get(self,index):
        """
        Return byte data by index(The No.index data of page.data).
        :param index: int, the index of data of page.data
        :return: byte, such as b'\x00\x00\x00\x00\x00\x00\x00\x0f' (for int 15)
        """
        return self.data[index*8:(index+1)*8]

    def updata(self, index, value):
        """
        Update byte data by index to value(Convert to byte)
        :param index: int
        :param value: int
        :return: None
        """
        self.data[index*8:(index+1)*8] = value.to_bytes(8, byteorder='big')

    def get_data_num(self):
        return self.num_records


class MultiPage():
    def __init__(self):
        self.index = 0   # Default page index = 0
        self.pages = [Page()] * MAXPAGE

    def get_current(self):
        return self.pages[self.index]

    def add_page_index(self):
        self.index += 1

    def last_page(self):
        return self.index == MAXPAGE - 1

    def get_page(self, key):
        if 0 <= key <= self.index:
            return self.pages[key]