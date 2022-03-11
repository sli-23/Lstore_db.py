from lstore.index import Index
import threading
from readerwriterlock import rwlock
from collections import defaultdict

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

class LockManager:

    def __init__(self):
        """
        HashMap: baserid -> RWLockFair
        """
        self.locks = defaultdict(Locks)

    """
    #Generate locks by base rid
    :param name: base_rid
    :param name: lock_type ('reader', 'writer')
    """

    def acquire(self, base_rid, lock_type):
        if lock_type == 'reader':
            return self.locks[base_rid].readLock()
        elif lock_type == 'writer':
            return self.locks[base_rid].writeLock()

    def release(self, base_rid, lock_type):
        if lock_type == 'reader':
            return self.locks[base_rid].releaseReadLock()
        elif lock_type == 'writer':
            return self.locks[base_rid].releaseWriteLock()

class Locks:

    def __init__(self):
        self.lock = threading.Lock()
        self.reading = False
        self.writing = False

    def readLock(self):         # get read lock
        self.lock.acquire()

        if self.writing:        # can't read if writing
            self.lock.release()
            return False
        else:
            self.reading = True
            self.lock.release()
            return True

    def releaseReadLock(self):
        self.lock.acquire()
        self.reading = False
        self.lock.release()

    def writeLock(self):
        self.lock.acquire()

        if self.reading:        # if something is reading, can't write
            self.lock.release()
            return False
        elif self.writing:      # if something else is writing, can't write
            self.lock.release()
            return False
        else:
            self.writing = True
            self.lock.release()
            return True

    def releaseWriteLock(self):
        self.lock.acquire()
        self.writing = False
        self.lock.release()

class Transaction:

    def __init__(self):
        self.queries = []
        self.locks = {}
        pass

    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            args = list(args)
            print(query, args)
            query(*args)
            
            query_object = query.__self__
            table = query.__self__.table
            
            """
            In the index, we use primary key to find base rid (when create a table, only primary key's index is allowed)
              - index.locate(column_num, key)
              - args[0] is always the primary key
            """
            base_rid = table.index.locate(table.key, args[0])[0]
            lock = self.locks.get(base_rid, None)
            
            """
            lock_type:
               reader: query.select, query.sum
               writer: query.insert, query.update, query.increment
            """
            
            lock_type = None
            if query == query_object.select or query == query_object.sum:
                lock_type = 'reader'               # pseudo-code, if query is to read
                if lock == None:  # no lock
                    if table.lock_manager.acquire(base_rid, lock_type):     # pseudo-code, if cannot get read lock, abort
                        return self.abort()
                    else:
                        self.locks[base_rid] = lock_type            # mark hash map key for that rid as reading
            
            elif query == query_object.insert or query == query_object.update or query == query_object.increment:
                lock_type = 'writer'
                if lock == None:
                    if table.lock_manager.acquire(base_rid, lock_type):
                        return self.abort()
                    else:
                        self.locks[base_rid] = lock_type 

        # Safe to execute all ops : no need to do rollback
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for i, (base_rid, lock_type) in enumerate(self.locks.items()):
            query_object = self.queries[0][0].__self__
            query_object.table.lock_manager.release(base_rid, lock_type)

    def commit(self):
        # TODO: commit to database ( durability : write to disk )
        for i, (base_rid, lock_type) in enumerate(self.locks.items()):
            query_object = self.queries[0][0].__self__
            query_object.table.lock_manager.release(base_rid, lock_type)