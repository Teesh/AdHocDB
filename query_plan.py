import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *
import re
import time
from collections import defaultdict

import helper_func
import parse

def query_plan(table_list, where_condition, select_columns, where_columns):

    path = "./Datasets/Movies/"
    #get tables that are needed
    for table in table_list:
        #use pandas here to upload the tables into memory
        if(len(table) == 2):
            #renaming
            t = table[0] #actual file name
            rename = table[1]
            #lookup from select and where columns
            cols = "["
            cols_list = []
            if rename in select_columns:
                for i in select_columns[rename]:
                    cols = cols + "'" + i + "',"
                    cols_list.append(i)
            if rename in where_columns:
                for i in where_columns[rename]:
                    if i not in cols_list:
                        cols = cols + "'" + i + "',"
                        cols_list.append(i)
            cols = cols[0:-1] + ']'
            print("cols: "+cols)
            globals()[rename] = eval('pandas.read_csv("' +path+ t + '.csv")['+cols+']')
            eval(rename+".rename(columns=lambda x: x+'__"+rename+"', inplace=True)")

        else:
            cols = "["
            cols_list = []
            t = table[0]
            if t in select_columns:
                for i in select_columns[t]:
                    cols = cols + "'" + i + "',"
                    cols_list.append(i)
            if t in where_columns:
                for i in where_columns[t]:
                    if i not in cols_list:
                        cols = cols + "'" + i + "',"
                        cols_list.append(i)
            cols = cols[0:-1] + ']'
            #no renaming
            globals()[t] = eval('pandas.read_csv("' +path+ t + '.csv")['+cols+']')
            eval(t+".rename(columns=lambda x: x+'__"+t+"', inplace=True)")

    return eval_or(where_condition, select_columns)

