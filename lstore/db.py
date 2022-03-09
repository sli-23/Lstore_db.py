from lstore.table import Table
from lstore.bufferpool import *
from lstore.index import Index
from lstore.config import *
import pickle
import os
import shutil

class Database():

    def __init__(self):
        self.path = ""
        self.tables = {} 
        self.bufferpool = BufferPool() #the default buffer pool will be 1,000 pages or 4 MB
        self.primary_key = {}

    # TODO: bufferpool stored in disk / merge
    def open(self, path):
        try:
            self.path = path
            #restore if there
            if not os.path.exists(path):
                os.makedirs(path)
            self.bufferpool.initial_path(path)

            tables = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
            #print(tables)
            for t_name in tables:
                t_path = os.path.join(path, t_name)
                old_table_metas = read_table(t_path)
                self.create_table(*old_table_metas)
            
            key_file = open(path + '/Primary_Key', 'wb')
            key_file.close()
        except:
            key_file = open(path + '/Primary_Key', 'rb')
            key_file.close()

    def keydict(self, table_name, table):
        key = table.key
        column = table.page_directory['base'][DEFAULT_COLUMN + key]
        self.primary_key[table_name] = column

    def close(self):
        for name, table in self.tables.items():
            self.keydict(name, table)
        
        #write primary key
        for key in self.primary_key.keys():
            primary_k = self.primary_key[key]
            key_file = open(self.path + '/' + key + '.tableKey', 'wb')
            pickle.dump(primary_k, key_file)
            key_file.close()
        
        #write table file
        for key in self.tables.keys():
            table = self.tables[key]
            #table.close() #it will trigger merger and evict all
            table_name = table.name
            #Rest the page lock and rid lock
            table.page_locks = None
            table.rid_locks = None
            table_path = os.path.join(self.bufferpool.path, table_name)
            write_table(table_path, table)

        """
        for key in self.tables.keys():
            table = self.tables[key]
            print('Closing the Table...')
            #table.close()
            #table.bufferpool.evict() #close the BufferPool
            tabledata_file = open(self.path + '/' + key + '.table', 'wb')
            pickle.dump(table, tabledata_file)
            tabledata_file.close()
        """
        #os.remove(self.path + '/' + 'Tables')
        os.remove(self.path + '/' + 'Primary_Key')


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        try:
            shutil.rmtree(name, ignore_errors=True)
        except:
            pass
        
        if name in self.tables.keys():
            print(f'table "{name}" exists...')
            table = self.tables[name]
        else:
            table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name not in self.tables.keys():      # Check whether table named "name" in tables, if not, print alert info,else delete the table.
            print(f'table {name} not exists.')
        # delete table file in the disk
        try:
            shutil.rmtree(name, ignore_errors=True)
            print('Drop table successful')
        except:
            pass

        del self.tables[name]
        return

    """
    # Returns table with the passed name
    
    if name in self.tables.keys():  # Check whether table named "name" in tables, if not, print alert info,else return the Table object.
        return self.tables[name]
    print(f'table {name} not exists.')
    """
    def get_table(self, name):
        path = self.path
        data = None

        #restore page directory
        if name in os.listdir(path):
            path = path + '/' + name
            fr = open(path, 'rb')
            data = pickle.load(fr)
            fr.close()
            name = Table(data[0], data[1], data[2])
            name.num_updates = data[3]
            name.num_records = data[4]
            name.page_directory = data[5]
            name.index = data[6]
            name.tail_index = data[7]
            name.indirection_index = data[8]
            name.bufferpool = data[9]
        else:
            print(f'table {name} not exists.')
            raise FileNotFoundError
        
        return name


def write_table(path, table):
    f = open(path, 'wb')
    metas = []
    metas.append(table.name)
    metas.append(table.num_columns)
    metas.append(table.key)
    metas.append(table.num_updates)
    metas.append(table.num_records)
    metas.append(table.page_directory)
    metas.append(table.index)
    metas.append(table.tail_index)
    metas.append(table.indirection_index)
    metas.append(table.bufferpool)
    pickle.dump(metas, f)
    f.close()

def read_table(path):
    f = open(path, "rb")
    metas = pickle.load(f)
    f.close()

    return metas
