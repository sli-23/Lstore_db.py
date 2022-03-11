from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        # new varible thread
        #self.thread = None
        pass

    """
    Done, Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run1(self):
        pass
        # here you need to create a thread and call __run
    
    """
    Done, Waits for the worker to finish
    """
    def join(self):
        #self.thread.join()
        pass

    """
    Done
    """
    def run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
