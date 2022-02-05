class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):                 # is this supposed to return an int or a bool - bool will be find 
        capacity = 4096 - self.num_records  # how many free indexes are available in data - Should we need to 4096/8 
        if capacity == 0:
            return False
        else:
            return True

    def write(self, value):                 # sets the an empty array index to value
        if has_capacity():
            self.data[self.num_records] = value
            self.num_records += 1
        else:
            raise ValueError('No capacity')