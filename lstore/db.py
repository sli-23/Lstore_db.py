from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = {}  # Use a hash map to store tables.

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables.keys():
            print(f'table "{name}" exists...')
            table = self.tables[name]
        else:
            table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table
        
    # not sure why the skeleton code has a return here; I figured "self.tables.append(table);" would be more appropriate after it's created.

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name not in self.tables.keys():      # Check whether table named "name" in tables, if not, print alert info,else delete the table.
            print(f'table {name} not exists.')
            return
        del self.tables[name]

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name in self.tables.keys():  # Check whether table named "name" in tables, if not, print alert info,else return the Table object.
            return self.tables[name]
        print(f'table {name} not exists.')
