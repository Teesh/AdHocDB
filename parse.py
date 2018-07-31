import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *
import re
import time
from collections import defaultdict

import helper_func

def query_plan(table_list, where_condition, select_columns, where_columns):
    
    path = "Datasets/Movies/"
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

def parsing(query):
    start = time.time()
    select = -1
    from_ind = -1
    where_ind = -1
    parsed = sqlparse.parse(query)[0]

    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            select = parsed.tokens.index(item)
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_ind = parsed.tokens.index(item)
        elif item.ttype is None and isinstance(item, Where):
            where_ind = parsed.tokens.index(item)

    if (where_ind > 0):
        select_columns = get_select_names(parsed, select, from_ind)
        from_tables = helper_func.get_table_names(parsed, from_ind, where_ind)
        where_condition = get_conditions(parsed, where_ind)
        where_columns = get_column(where_condition)
    else:
        select_columns = get_select_names(parsed, select, from_ind)
        from_tables = helper_func.get_table_names(parsed, from_ind, len(parsed.tokens))


    select_dict = select_col_dict(select_columns)
    #figure out how exactly to do the computations
    result = query_plan(from_tables, where_condition, select_dict, where_columns)
    final = projection(result, select_dict)

    end = time.time()
    print("Time:", end-start)
    print(final)

def projection(table, columns):
    '''
    get the final result table and a list of columns
    '''
    cols = []
    for key in columns:
        for v in columns[key]:
            table_plus_col = v+"__"+key
            if v in table:
                cols.append(v)
            elif table_plus_col in table:
                cols.append(table_plus_col)
    return table[cols]


def eval_or(conditions, columns):
    cond_lower = [c.lower() for c in conditions]
    if 'or' in cond_lower:
        a = cond_lower.index('or')
        left = conditions[0:a]
        right = conditions[a+1:]
        left_eval = eval_and(left)
        right_eval = eval_or(right, columns)
        return combine_or(left_eval, right_eval, columns)
    else:
        return eval_and(conditions)



def combine_or(table1, table2, columns):
    result = table1.append(table2)
    return result


def eval_and(conditions):
    cond_lower = [c.lower() for c in conditions]
    if 'and' in cond_lower:
        a = cond_lower.index('and')
        left = conditions[0:a]
        right = conditions[a+1:]
        #continue down the right subtree, and get a resulting table
        right_table = eval_and(right)
        #if there is a not, then helper_func.negate the condition
        left_cond = eval_not(left)
        #compbine table from right with left cond
        return combine_and(left_cond, right_table)
    else:
        #last condition, helper_func.negate and then evaluate
        cond = eval_not(conditions)
        return eval_cond(cond)


def eval_not(condition):
    cond_lower = [c.lower() for c in condition]
    if 'not' in cond_lower:
        a = cond_lower.index('not')
        neg = helper_func.negate(condition[a+1:])
        return neg
    else:
        return condition


def combine_and(left_cond, right_result):
    left,binop,right = comparision_parse(left_cond)
    left_left, arithm_op, left_right, binop, right = arithm_parse_eval(left,binop,right)
    ntable, left_table, right_table, left_col, right_col = helper_func.check_num_table(left_left, right)

    if left_col not in right_result.columns:
        #completely disjoint
        #evaluate left
        left_result = eval_cond(left_cond)
        #cross join with right
        left_result['tmp'] = 1
        right_result['tmp'] = 1

        out = pandas.merge(left_result, right_result, on='tmp')
        return eval("out.query('"+left_col + binop + right_col+"')")
    else:
        #left table should have been part of computations in right subtree
        if arithm_op != None:
            new_col = eval(right_result+"["+left_col +"]"+arithm_op+left_left)
            right_result.update(eval("pandas.DataFrame({'"+left_col+"': new_col})"))
        #join using tmp and col with table from right subtree and right col
        if ntable == 1 and left_col in right_result.columns:
            #simple filter because column already in right_table
            if right_col is None:
                return eval('right_result.query("'+left_col + binop + right+'")')
            else:
                return eval("right_result.query('"+left_col + binop + right_col+"')")
        elif ntable == 2 and left_col in right_result.columns:
            #merge and filter
            #left_col already in right_result
            right_result['tmp'] = 1
            rt = eval(right_table)
            rt['tmp'] = 1
            out = pandas.merge(right_result, rt, on='tmp')
            return eval("out.query('"+left_col + binop + right_col+"')")
            pass



def eval_cond(condition):
    result = []
    left,binop,right = comparision_parse(condition)
    left_left, arithm_op, left_right, binop, right = arithm_parse_eval(left,binop,right)
    ntable, left_table, right_table, left_col, right_col = helper_func.check_num_table(left_left, right)
    if ntable == 1:
        #we can evaluate the condition here itself and return
        return eval(left_table + '.query("'+left_col + binop + right+ '")')
    
    else:
        tmp = eval(left_table)
        #eval arithm operator and replace left table with tmp
        if arithm_op != None:
            new_col = eval(left_table+"["+left_col+"]"+arithm_op+left_left)
            tmp.update(eval("pandas.DataFrame({'"+left_col+"': new_col})"))
        #join
        if binop == '=' or binop == '==':
            out = eval('tmp.merge('+right_table+', left_on="'+left_col+'", right_on="'+right_col+'", how = "inner")')
            return out
        else:
            tmp['tmp'] = 1
            # tmpr = eval(right_table + "['tmp'] = 1")
            tmpr = eval(right_table)
            tmpr['tmp'] = 1
            out = pandas.merge(tmp, tmpr, on='tmp')
            # out = eval(left_table+'.merge('+right_table+', left_on="'+left_col+'", right_on="'+right_col+'")')
            return eval("out.query('"+left_col + binop + right_col+"')")
        pass




def create_cond_str(where_condition):
    cond_str = "["
    for w in where_condition:
        if w.upper() == "AND":
            cond_str = cond_str + ' & '
        elif w.upper() == 'OR':
            cond_str = cond_str + ' | '
        elif w.upper() == 'NOT':
            cond_str = cond_str + ' !'
        else:
            ise = helper_func.find_char_pos(w, '=')
            if ise != -1:
                w = w[0:ise] + "==" + w[ise+1:]
            cond_str = cond_str + 'eval("' + w + '")'
    cond_str = cond_str + ']'
    return cond_str


def get_select_names(parsed,start,stop):
    token_stream = parsed.tokens[start:stop]
    for item in token_stream:
        if isinstance(item, IdentifierList) or isinstance(item, Identifier):
            x = item.value.split(',')
            y = [t.strip() for t in x]
            return y

def select_col_dict(inputtables):
    dictionary = defaultdict(set)
    for item in inputtables:
        dot = helper_func.find_char_pos(item, '.')
        if dot != -1:
            t = item[0:dot]
            c = item[dot+1:]
            if t in dictionary:
                dictionary[t].add(c)
            else:
                dictionary[t].add(c)
    return dictionary


def get_conditions(parsed, start):
  '''
  Gets the conditions from the where clause and returns them in a list
  The conditions are kept in the same order as written and the operators
  are also there in the same order

  Returns: List of strings
  '''
  token_stream = parsed.tokens[start:]
  parsed_where = next(token for token in token_stream if isinstance(token, Where))[1:]
  # print(parsed_where[1].value)
  conditions = []
  for token in parsed_where:
    if token.ttype is Keyword or token.ttype is Operator:
      conditions.append(token.value)
    if token.ttype is None:
      conditions.append(token.value)
  # print(conditions)
  return conditions


def arithm_parse_eval(left,op,right):
    '''
        Assuming that we only have one arithm op on left side
    '''
    left_left = left
    left_right = None
    arithm_op = None
    binary_op = op
    for char in left:
        if helper_func.find_char_pos(left, '+') != -1 :
            op_side = "left"
            left_left = left[0:helper_func.find_char_pos(left, '+')]
            left_right = left[helper_func.find_char_pos(left, '+')+1:]
            arithm_op = "+"
        if helper_func.find_char_pos(left, '-') != -1 :
            op_side = "left"
            left_left = left[0:helper_func.find_char_pos(left, '+')]
            left_right = left[helper_func.find_char_pos(left, '+')+1:]
            arithm_op = "-"
        if helper_func.find_char_pos(left, '*') != -1 :
            op_side = "left"
            left_left = left[0:helper_func.find_char_pos(left, '+')]
            left_right = left[helper_func.find_char_pos(left, '+')+1:]
            arithm_op = "*"
        if helper_func.find_char_pos(left, '/') != -1 :
            op_side = "left"
            left_left = left[0:helper_func.find_char_pos(left, '+')]
            left_right = left[helper_func.find_char_pos(left, '+')+1:]
            arithm_op = "/"
    return (left_left, arithm_op, left_right, binary_op, right)




def comparision_parse(item):
    left = None
    op = None
    right = None
    item = item[0]
    if (helper_func.find_char_pos(item, '<') != -1 and helper_func.find_char_pos(item, '=') != -1):
            left, op, right = (item[0:helper_func.find_char_pos(item, '<')]),"<=",(item[helper_func.find_char_pos(item, '=')+1:])
    elif (helper_func.find_char_pos(item, '>') != -1 and helper_func.find_char_pos(item, '=') != -1):
            left, op, right =  (item[0:helper_func.find_char_pos(item, '>')]),">=",(item[helper_func.find_char_pos(item, '=')+1:])
    elif (helper_func.find_char_pos(item, '<') != -1 and helper_func.find_char_pos(item, '>') != -1):
            left, op, right =  (item[0:helper_func.find_char_pos(item, '<')]),"<>",(item[helper_func.find_char_pos(item, '>')+1:])
    elif helper_func.find_char_pos(item, '<') != -1:
            left, op, right =  (item[0:helper_func.find_char_pos(item, '<')]),"<",(item[helper_func.find_char_pos(item, '<')+1:])
    elif helper_func.find_char_pos(item, '>') != -1:
            left, op, right =  (item[0:helper_func.find_char_pos(item, '>')]),">",(item[helper_func.find_char_pos(item, '>')+1:])
    elif helper_func.find_char_pos(item, '=') != -1:
            left, op, right =  (item[0:helper_func.find_char_pos(item, '=')]),"==",(item[helper_func.find_char_pos(item, '=')+1:])
    return (left, op, right)

def get_column_helper(item):
    if helper_func.find_char_pos(item, '.') != -1:
        return(item[0:helper_func.find_char_pos(item, '.')],item[helper_func.find_char_pos(item, '.')+1:])
    return (None, None)

def get_column(condition):
    column = []
    dictionary = defaultdict(set)
    for item in condition:
        item = helper_func.rm_white(item)
        if item.lower() == 'and' or item.lower == 'or' or item.lower == 'not':
            continue
        left,op,right = comparision_parse([item])
        if left is not None:
            key,text = get_column_helper(left)
            if key == None:
                continue
            if key in dictionary:
                dictionary[key].add(text)
            else:
                dictionary[key].add(text)
#            if text is not None:
#                column.append(text)
        if right is not None:
            key,text = get_column_helper(right)
            if key == None:
                continue
            if key in dictionary:
                dictionary[key].add(text)
            else:
                dictionary[key].add(text)
#            if text is not None:
#                column.append(text)
    return dictionary


