from lstore.BPlusTree import BPlusTree

a = BPlusTree()
value = 5
key = 1
for i in range(50):
    a.insert(key + i, value + i)

print(a.delete(10))
print(a.delete(10))
a.insert(10, 5)
print(a.retrieve(10)[0])