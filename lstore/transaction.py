from lstore.table import Table, Record
from lstore.index import Index
import threading
from readerwriterlock import rwlock

"""
Priority:
RR
RW - ReleaseReaderLock()
WW - RealeaseWriterLock()
WR - ReleaseWriterLock()

Reader priority:
rwlock.RWLockReaed()

Write priority:
rwlock.RWLockWrite()

Fair priority:
rwlock.RWLockFair()
"""
#RWLockFair() reading lock

class Locks:
    def __init__(self):
        self.locks = {}

    def check_locks(self, rid):
        return self.locks[rid]

    def acquire_reader(self, rid):
        if self.check_locks(rid):
            ReaderLock = self.locks[rid].gen_rlock()
            success = ReaderLock.acquire(blocking=False)
            if success:
                return ReaderLock
            else:
                return None
        else:
            self.locks[rid] = rwlock.RWLockFair()

    def release_reader(self, rid):
        return self.locks[rid].release()
    
    def acquire_writer(self,rid):
        if self.check_locks(rid):
            WriteLock = self.locks[rid].gen_wlock()
            success = WriteLock.acquire(blocking=False)
            if success:
                return WriteLock
            else:
                return None
        else:
            self.locks[rid] = rwlock.RWLockFair()
    
    def release_writer(self,rid):
        return self.locks[rid].release()


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.table = None
        self.queries = []
        self.locks = {}
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, table_name, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, table, query, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction
    # commits or False on abort
    def run(self):
        for query, args in self.queries:
            #using index - primary key - base_rid
            query_object = query.__self__
            base_rid = query_object.table.index.locate(query_object.table.key, args[0])
            
            #Query type:
            if query == query_object.select:                 
                lock_type = 'reader'
                self.locks[base_rid] = lock_type

            if query == query_object.insert:            
                lock_type = 'writer'
            
            if query == query_object.update:
                lock_type = 'writer'

            if query == query_object.sum:
                lock_type = 'reader'

            if query == query_object.increment:
                lock_type = 'writer'

        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if not result:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database
        return True

