import pandas
from BTrees.OOBTree import OOBTree
import pickle
t = OOBTree()

path = "Datasets/Reviews/"

#class btreeNode:
#    def __init__(self):
#        self.leaf = None
#        self.key = []
#        self.children = None
#        self.values = []
#    def insert_value(self,value):
#        if self.leaf == True:
#            for item in self.values:
#                for element in item:
#                    if element[0] == value[0]:
#                        element.append(value)
#    def insert_key(self,key):
#        self.key.append(key)
#    def split(self):
#        if len(key) >=5 or len(values) >=5:
#            left = btreeNode()
#            right = btreeNode()
class content:
    def __init__(self):
        self.row_num = []
        self.business_id = []
    def insert(self,row,b_id):
        self.row_num.append(row)
        self.business_id.append(b_id)
        

        
datas = pandas.read_csv(path+"review-1m.csv")
for i in range (datas.shape[0]):
    print(i)
    row = i
    b_id = datas.iloc[row]['business_id']
    key = datas.iloc[row]['useful']
    if t.has_key(key):
        c = t.__getitem__(key)
        c.insert(row,b_id)
        t.update({key:c})
    else:
        c = content()
        c.insert(row,b_id)
        t.update({key:c})
        
funny_file = open(r'../test/useful.pkl', 'wb')
pickle.dump(t, funny_file)
funny_file.close()
#c = content(1)
#c.insert(1,100,200)
#c.insert(1,200,300)
#print(c.key,c.row_num,c.business_id)
#
#t.update({1:c})
#c = t.__getitem__(1)
#print(c.key,c.row_num,c.business_id)
#c.insert(1,300,400)
#t.update({1:c})
#c = t.__getitem__(1)
#print(c.key,c.row_num,c.business_id)