class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):                 # is this supposed to return an int or a bool
        capacity = 4096 - self.num_records  # how many free indexes are available in data
        if capacity == 0:
            return false
        else:
            return true

    def write(self, value):                 # sets the an empty array index to value
        self.data[self.num_Records] = value
        self.num_records += 1
