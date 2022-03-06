from lstore.table import Table, Record
from lstore.index import Index
import threading


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

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.locks = {}
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, table, query, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction
    # commits or False on abort
    def run(self):
        for query, args in self.queries:
            rid = ???                               # pseudo-code
            lock = self.locks.get(rid, None)

            if query == read_query:                 # pseudo-code, if query is to read
                if lock == None:     # no lock
                    if (get readLock == False):     # pseudo-code, if cannot get read lock, abort
                        return self.abort()
                    else:
                        locks[rid] = 'r'            # mark hash map key for that rid as reading

            elif query == write_query:              # pseudo-code, if query is to write
                if lock == None:     # no lock
                    if (get writeLock == False):    # pseudo-code, if cannot get write lock, abort
                        return self.abort()
                    else:
                        locks[rid] = 'w'            # mark hash map key for that rid as writing
                elif lock == 'reading':
                    releaseReadLock()               # pseudo-code, release the read lock
                    if (get writeLock == False):    # pseudo-code, if cannot get write lock, abort
                        return self.abort()
                    else:
                        locks[rid] = 'w'            # mark hash map key for that rid as writing

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

