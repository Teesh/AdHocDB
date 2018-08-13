import pandas
from BTrees.OOBTree import OOBTree
import pickle
t = OOBTree()

class content:
    def __init__(self):
        self.row_num = []
        self.business_id = []
    def insert(self,row,b_id):
        self.row_num.append(row)
        self.business_id.append(b_id)
        
file2 = open(r'../test/funny.pkl', 'rb')
t = pickle.load(file2)
file2.close()
#print(list(t.values(8)))
for item in list(t.values(8,8)):
    print(item.row_num,item.business_id)