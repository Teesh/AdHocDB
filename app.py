import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *
import re
import time

conds = ["movies.movie_title == 'King Kong'","movies.actor_1_facebook_likes < 20000","actor_2_facebook_likes > actor_1_facebook_likes","movies.title_year < 2010"]

#movies = pandas.read_csv('./movies.csv')

#print(movies[eval(conds[1]) & eval(conds[3])])

path = "Datasets/Movies/"

def main():
    parser = argparse.ArgumentParser(description='Preprocessing files and running queries.')
    parser.add_argument('--preprocess', metavar = 'Files', nargs='+', type=str,
                      help='Preprocess files where files are given in a space seperated list. Ex. File1 File2 ...')
    parser.add_argument('--query', type=str,  nargs='+',
                      help='Run a query with the syntax "SELECT <columns> FROM <tables> WHERE <condition>". Note: The quotations around the SELECT statement are very important.')

    args = parser.parse_args()
    query = args.query
    preprocess = args.preprocess
    if preprocess is None:
        #go to query function since no preprocessing needed
        query = ' '.join(query)
        parsing(query)

    if query is None:
        #go to preprocessing function
        preprocessing()


def preprocessing():
    pass

def parsing(query):
    start = time.time()
    select = -1
    from_ind = -1
    where_ind = -1
    parsed = sqlparse.parse(query)[0]
    # print(parsed.tokens)

    for item in parsed.tokens:
    # print(item.ttype)
        if item.ttype is DML and item.value.upper() == 'SELECT':
            select = parsed.tokens.index(item)
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_ind = parsed.tokens.index(item)
        elif item.ttype is None and isinstance(item, Where):
            where_ind = parsed.tokens.index(item)

    print(select, from_ind, where_ind, len(parsed.tokens))

    if (where_ind > 0):
      	select_columns = get_select_names(parsed, select, from_ind)
      	from_tables = get_table_names(parsed, from_ind, where_ind)
      	where_condition = get_conditions(parsed, where_ind)
    else:
      	select_columns = get_select_names(parsed, select, from_ind)
      	from_tables = get_table_names(parsed, from_ind, len(parsed.tokens))

    #figure out how exactly to do the computations
    result = query_plan(from_tables, where_condition)
    print(select_columns)
    print(projection(result, select_columns))

    end = time.time()
    print("Time:", end-start)


def projection(table, columns):
    '''
    get the final result table and a list of columns
    '''
    return table[columns]


def query_plan(table_list, where_condition):
    #get tables that are needed
    for table in table_list:
        #use pandas here to upload the tables into memory
        print(table)
        if(len(table) == 2):
            globals()[table[1]] = eval('pandas.read_csv("' +path+ table[0] + '.csv")')
        else:
            print(table[0])
            globals()[table[0]] = eval('pandas.read_csv("' +path+ table[0] + '.csv")')


    if len(table_list) == 1:
        if(len(table_list[0]) == 1):
            cond_str = table_list[0][0] + create_cond_str(where_condition)
        else:
            cond_str = table_list[0][1] + create_cond_str(where_condition)
        return eval(cond_str)

    else:
        return eval_or(where_condition)


def eval_or(conditions):
    cond_lower = [c.lower() for c in conditions]
    try:
        a = cond_lower.index('or')
        left = conditions[0:a]
        right = conditions[a+1:]
        left_eval = eval_and(left)
        right_eval = eval_or(right)
        return combine_or(left, right)
    except ValueError:
        return eval_and(conditions)

def eval_and(conditions):
    cond_lower = [c.lower() for c in conditions]
    try:
        a = cond_lower.index('and')
        left = conditions[0:a]
        right = conditions[a+1:]
        left_eval = eval_not(left)
        right_eval = eval_and(right)
        return combine_and(left, right)
    except ValueError:
        return eval_not(conditions)


def eval_not(conditions):
    cond_lower = [c.lower() for c in conditions]
    try:
        a = cond_lower.index('not')
        neg = negate(conditions[a+1:])
        return eval_cond(neg)
    except ValueError:
        return eval_cond(conditions)


def eval_cond(conditions):
    result = []
    left,op,right = comparision_parse(conditions)
    processed = arithm_parse(left,op,right)
    result.append(processed)
    return result


def combine_and(conditions):
    pass

def combine_or(conditions):
    pass

def negate(conditions):
    left,op,right = comparision_parse(conditions)
    if op == "=":
        op = "<>"
    elif op == "<>":
        op = "="
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

def create_cond_str(where_condition):
    print(where_condition)
    cond_str = "["
    for w in where_condition:
        if w.upper() == "AND":
            cond_str = cond_str + ' & '
        elif w.upper() == 'OR':
            cond_str = cond_str + ' | '
        elif w.upper() == 'NOT':
            cond_str = cond_str + ' !'
        else:
            cond_str = cond_str + 'eval("' + w + '")'
    cond_str = cond_str + ']'
    print(cond_str)
    return cond_str

def parse_where(condition):
    '''
    count how many dots to determine
    return t (list of strings): the table names in that query
           c (list of columns): the columns used in that query if any
    '''

    pass

def get_select_names(parsed,start,stop):
    print("in select names")
    token_stream = parsed.tokens[start:stop]
    print(token_stream)
    for item in token_stream:
        if isinstance(item, IdentifierList) or isinstance(item, Identifier):
            x = item.value.split(',')
            y = [t.strip() for t in x]
            return y

def get_table_names(parsed, start, stop):
    '''
    Returns the names of the columns after the SELECT keyword or
    the names of the tables after the FROM keyword

    Input:
        parsed List: Parsed query tokens
        start int: starting point of SELECT or FROM
        stop int: ending point of SELECT or From statement

    Returns: [["table1", "t1"],["table2", "t2"]...]
    '''
    token_stream = parsed.tokens[start:stop]
    print(token_stream)
    for item in token_stream:
        if isinstance(item, IdentifierList) or isinstance(item, Identifier):
            tables = item.value.split(',')
            y = [t.strip().split(' ') for t in tables]
            return y


def get_conditions(parsed, start):
  '''
  Gets the conditions from the where clause and returns them in a list
  The conditions are kept in the same order as written and the operators
  are also there in the same order

  Returns: List of strings
  '''
  token_stream = parsed.tokens[start:]
  parsed_where = next(token for token in token_stream if isinstance(token, Where))[1:]
  print(parsed_where)
  # print(parsed_where[1].value)
  conditions = []
  for token in parsed_where:
    if token.ttype is Keyword or token.ttype is Operator:
      conditions.append(token.value)
    if token.ttype is None:
      conditions.append(token.value)
  # print(conditions)
  return conditions

def condition_check(conditions):
  count = 0
  result = []
  for item in conditions:
    if item.upper() != "AND" and item.upper() != "OR" and item.upper() != "NOT":
        left,op,right = comparision_parse(item)
        processed = arithm_parse(left,op,right)
        result.append(processed)
        count = count +1
    else:
        result.append(item)
  result.insert(0,count)
  return result

def arithm_parse(left,op,right):
    for char in left:
        if find_char_pos(left, '+') != -1 :
            return ("left",left[0:find_char_pos(left, '+')],"+",left[find_char_pos(left, '+')+1:],op,right)
        if find_char_pos(left, '-') != -1 :
            return ("left",left[0:find_char_pos(left, '-')],"-",left[find_char_pos(left, '-')+1:],op,right)
        if find_char_pos(left, '*') != -1 :
            return ("left",left[0:find_char_pos(left, '*')],"*",left[find_char_pos(left, '*')+1:],op,right)
        if find_char_pos(left, '/') != -1 :
            return ("left",left[0:find_char_pos(left, '/')],"/",left[find_char_pos(left, '/')+1:],op,right)
    for char in right:
        if find_char_pos(right, '+') != -1 :
            return ("right",left,op,right[0:find_char_pos(right, '+')],"+",right[find_char_pos(right, '+')+1:])
        if find_char_pos(right, '-') != -1 :
            return ("right",left,op,right[0:find_char_pos(right, '-')],"-",right[find_char_pos(right, '-')+1:])
        if find_char_pos(right, '*') != -1 :
            return ("right",left,op,right[0:find_char_pos(right, '*')],"*",right[find_char_pos(right, '*')+1:])
        if find_char_pos(right, '/') != -1 :
            return ("right",left,op,right[0:find_char_pos(right, '/')],"/",right[find_char_pos(right, '/')+1:])
    return ("no_op",left,op,right)

def check_num_table(left, right):
    
    pass

def filter_comp():
    pass

def merge_comp():
    pass

def comparision_parse(item):
    if (find_char_pos(item, '<') != -1 and find_char_pos(item, '=') != -1):
            side, left, op, right = (item[0:find_char_pos(item, '<')]),"<=",(item[find_char_pos(item, '=')+1:])
            ntable = check_num_table(left, right)
    elif (find_char_pos(item, '>') != -1 and find_char_pos(item, '=') != -1):
            side, left, op, right =  (item[0:find_char_pos(item, '>')]),">=",(item[find_char_pos(item, '=')+1:])
            ntable = check_num_table(left, right)
    elif (find_char_pos(item, '<') != -1 and find_char_pos(item, '>') != -1):
            side, left, op, right =  (item[0:find_char_pos(item, '<')]),"<>",(item[find_char_pos(item, '>')+1:])
            ntable = check_num_table(left, right)
    elif find_char_pos(item, '<') != -1:
            side, left, op, right =  (item[0:find_char_pos(item, '<')]),"<",(item[find_char_pos(item, '<')+1:])
            ntable = check_num_table(left, right)
    elif find_char_pos(item, '>') != -1:
            side, left, op, right =  (item[0:find_char_pos(item, '>')]),">",(item[find_char_pos(item, '>')+1:])
            ntable = check_num_table(left, right)
    elif find_char_pos(item, '=') != -1:
            side, left, op, right =  (item[0:find_char_pos(item, '=')]),"=",(item[find_char_pos(item, '=')+1:])
            ntable = check_num_table(left, right)

    if ntable == 1:
        filter_comp()
    else:
        merge_comp()

def find_char_pos(string, char):
    if char in string:
        return string.find(char)
    else:
        return -1

if __name__ == "__main__":
    main()
