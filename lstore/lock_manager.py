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
