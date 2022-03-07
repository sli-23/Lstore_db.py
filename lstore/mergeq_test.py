from queue import Queue
from collections import OrderedDict
import struct
mergeQ = Queue()
print(mergeQ.empty())

test = OrderedDict()
test['1'] = 1
test['2'] = 2


print(struct.pack_into('>I', 1, 0, 1))