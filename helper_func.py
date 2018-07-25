import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *
import re
import time
from collections import defaultdict

def find_char_pos(string, char):
    if char in string:
        return string.find(char)
    else:
        return -1
    
def rm_white(string):
    return string.replace(" ", "")


def check_num_table(left, right):
    table_l = find_char_pos(left, '.')
    table_r = find_char_pos(right, '.')
    left_t = left[0:table_l]
    left_col = left[table_l + 1:]
    left_t = left_t.strip()
    left_col = left_col.strip() + "__" + left_t
    if table_r != -1:
        right_t = right[0:table_r]
        right_col = right[table_r + 1:]
        right_t = right_t.strip()
        right_col = right_col.strip() + "__" + right_t
        if left_t == right_t:
            return (1, left_t, right_t, left_col, right_col)
        else:
            return (2, left_t, right_t, left_col, right_col)
    else:
        return (1, left_t, None, left_col, None)

    
def get_table_names(parsed, start, stop):
    '''
    Returns the names of the columns after the FROM keyword

    Input:
        parsed List: Parsed query tokens
        start int: starting point of SELECT or FROM
        stop int: ending point of SELECT or From statement

    Returns: [["table1", "t1"],["table2", "t2"]...]
    '''
    token_stream = parsed.tokens[start:stop]
    for item in token_stream:
        if isinstance(item, IdentifierList) or isinstance(item, Identifier):
            tables = item.value.split(',')
            y = [t.strip().split(' ') for t in tables]
            return y
        
def negate(conditions):
    left,op,right = comparision_parse(conditions)
    if op == "=" or op == "==":
        op = "<>"
    elif op == "<>":
        op = "=="
    elif op == "<=":
        op = ">="
    elif op == ">=":
        op = "<="
    elif op ==">":
        op = "<"
    elif op == "<":
        op = ">"
    result = []
    result = append(left)
    result = append(op)
    result = append(right)
    return (''.join(result))

def rename_columns(table):
    columns = list(eval(table))
    newcol = {}
    for c in columns:
        newcol[c] = table + '.'+c
    globals()[table] = eval(table + ".rename(columns=newcol)")