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
    result = query_plan(from_tables, where_condition, select_columns)
    print(select_columns)
    print(projection(result, select_columns))

    end = time.time()
    print("Time:", end-start)


def projection(table, columns):
    '''
    get the final result table and a list of columns
    '''
    return table[columns]


def query_plan(table_list, where_condition, select_columns):
    #get tables that are needed
    for table in table_list:
        #use pandas here to upload the tables into memory
        print(table)
        if(len(table) == 2):
            globals()[table[1]] = eval('pandas.read_csv("' +path+ table[0] + '.csv", index_col=False)')
        else:
            print(table[0])
            globals()[table[0]] = eval('pandas.read_csv("' +path+ table[0] + '.csv", index_col=False)')


    if len(table_list) == 1:
        if(len(table_list[0]) == 1):
            cond_str = table_list[0][0] + create_cond_str(where_condition)
        else:
            cond_str = table_list[0][1] + create_cond_str(where_condition)
        return eval(cond_str)

    else:
        return eval_or(where_condition, select_columns)


def eval_or(conditions, columns):
    cond_lower = [c.lower() for c in conditions]
    try:
        a = cond_lower.index('or')
        left = conditions[0:a]
        right = conditions[a+1:]
        left_eval = eval_and(left)
        right_eval = eval_or(right)
        return combine_or(left, right, columns)
    except ValueError:
        return eval_and(conditions)


def combine_or(table1, table2, columns):
    result = table1.append(table2)
    return result[columns]


def eval_and(conditions):
    cond_lower = [c.lower() for c in conditions]
    try:
        a = cond_lower.index('and')
        left = conditions[0:a]
        right = conditions[a+1:]
        #continue down the right subtree, and get a resulting table
        right_table = eval_and(right)
        #if there is a not, then negate the condition
        left_cond = eval_not(left)
        #compbine table from right with left cond
        return combine_and(left_cond, right_table)
    except ValueError:
        #last condition, negate and then evaluate
        cond = eval_not(conditions)
        return eval_cond(cond)


def eval_not(condition):
    cond_lower = [c.lower() for c in condition]
    try:
        a = cond_lower.index('not')
        neg = negate(condition[a+1:])
        return neg
    except ValueError:
        return condition


def combine_and(left_cond, right_result):
    left,binop,right = comparision_parse(left_cond)
    left_left, arithm_op, left_right, binop, right = arithm_parse(left,binop,right)
    ntable, left_table, right_table, left_col, right_col = check_num_table(left_left, right)
    #left table should have been part of computations in right subtree
    if arithm_op != None:
        new_col = eval(right_result+"["+left_col"]"+arithm_op+left_left)
        right_result.update(eval("pandas.DataFrame({'"left_col"': new_col})"))
    #join using tmp and col with table from right subtree and right col
    if ntable == 1 and left_col in right_result.columns:
        #simple filter because column already in right_table
        return(eval("right_result["+left_col+binop+right"]"))
    else if ntable == 1 and left_col not in right_result.columns:
        #join tables then filter
        out = eval('right_result.merge('+left_table+')')
        result = eval("out["left_col+binop+right"]")
        return result
    else:
        #join and filter
        out = eval('right_result.merge('+right_table+')')
        result = eval("out["left_col+binop+right"]")
        return result


def eval_cond(condition):
    result = []
    left,binop,right = comparision_parse(conditions)
    left_left, arithm_op, left_right, binop, right = arithm_parse(left,binop,right)
    ntable, left_table, right_table, left_col, right_col = check_num_table(left_left, right)
    if ntable == 1:
        #we can evaluate the condition here itself and return
        cond_str = create_cond_str(condition)
        return eval(cond_str)
    else:
        tmp = eval(left_table)
        #eval arithm operator and replace left table with tmp
        if arithm_op != None:
            new_col = eval(left_table+"["+left_col"]"+arithm_op+left_left)
            tmp.update(eval("pandas.DataFrame({'"left_col"': new_col})"))
        #join
        if binop == '=':
            out = eval('tmp.merge('+right_table+', left_on="'+left_col+'", right_on="'+right_col+'")')
            return out
        else:
            out = eval('tmp.merge('+right_table+')')
            return eval("out["left_col+binop+right"]")
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
    Returns the names of the columns after the FROM keyword

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

# def condition_check(conditions):
#   count = 0
#   result = []
#   for item in conditions:
#     if item.upper() != "AND" and item.upper() != "OR" and item.upper() != "NOT":
#         left,op,right = comparision_parse(item)
#         processed = arithm_parse(left,op,right)
#         result.append(processed)
#         count = count +1
#     else:
#         result.append(item)
#   result.insert(0,count)
#   return result

def arithm_parse_eval(left,op,right):
    '''
        Assuming that we only have one arithm op on left side
    '''
    # op_side = None
    left_left = left
    left_right = None
    arithm_op = None
    binary_op = op
    # right_left = right
    # right_right = None
    for char in left:
        if find_char_pos(left, '+') != -1 :
            op_side = "left"
            left_left = left[0:find_char_pos(left, '+')]
            left_right = left[find_char_pos(left, '+')+1:]
            arithm_op = "+"
            # return ("left",left[0:find_char_pos(left, '+')],"+",left[find_char_pos(left, '+')+1:],op,right)
        if find_char_pos(left, '-') != -1 :
            op_side = "left"
            left_left = left[0:find_char_pos(left, '+')]
            left_right = left[find_char_pos(left, '+')+1:]
            arithm_op = "-"
            # return ("left",left[0:find_char_pos(left, '-')],"-",left[find_char_pos(left, '-')+1:],op,right)
        if find_char_pos(left, '*') != -1 :
            op_side = "left"
            left_left = left[0:find_char_pos(left, '+')]
            left_right = left[find_char_pos(left, '+')+1:]
            arithm_op = "*"
            # return ("left",left[0:find_char_pos(left, '*')],"*",left[find_char_pos(left, '*')+1:],op,right)
        if find_char_pos(left, '/') != -1 :
            op_side = "left"
            left_left = left[0:find_char_pos(left, '+')]
            left_right = left[find_char_pos(left, '+')+1:]
            arithm_op = "/"
            # return ("left",left[0:find_char_pos(left, '/')],"/",left[find_char_pos(left, '/')+1:],op,right)
    # for char in right:
    #     if find_char_pos(right, '+') != -1 :
    #         op_side = "right"
    #         right_left = right[0:find_char_pos(right, '+')]
    #         right_right = right[find_char_pos(right, '+')+1:]
    #         arithm_op = "+"
    #         return ("right",left,op,right[0:find_char_pos(right, '+')],"+",right[find_char_pos(right, '+')+1:])
    #     if find_char_pos(right, '-') != -1 :
    #         return ("right",left,op,right[0:find_char_pos(right, '-')],"-",right[find_char_pos(right, '-')+1:])
    #     if find_char_pos(right, '*') != -1 :
    #         return ("right",left,op,right[0:find_char_pos(right, '*')],"*",right[find_char_pos(right, '*')+1:])
    #     if find_char_pos(right, '/') != -1 :
    #         return ("right",left,op,right[0:find_char_pos(right, '/')],"/",right[find_char_pos(right, '/')+1:])
    return (left_left, arithm_op, left_right, binary_op, right)


def check_num_table(left, right):
    table_l = find_char_pos(left, '.')
    table_r = find_char_pos(right, '.')
    if table_l != -1 and table_r != -1:
        left_t = left[0:table_l]
        right_t = right[0:table_r]
        left_col = left[table_l + 1:]
        right_col = right[table_r + 1:]
        if left_t == right_t:
            return (1, left_t, None, left_col, None)
        else:
            return (2, left_t, right_t, left_col, right_col)
    else:
        return (1, left_t, None, left_col, None)


def comparision_parse(item):
    if (find_char_pos(item, '<') != -1 and find_char_pos(item, '=') != -1):
            left, op, right = (item[0:find_char_pos(item, '<')]),"<=",(item[find_char_pos(item, '=')+1:])
    elif (find_char_pos(item, '>') != -1 and find_char_pos(item, '=') != -1):
            left, op, right =  (item[0:find_char_pos(item, '>')]),">=",(item[find_char_pos(item, '=')+1:])
    elif (find_char_pos(item, '<') != -1 and find_char_pos(item, '>') != -1):
            left, op, right =  (item[0:find_char_pos(item, '<')]),"<>",(item[find_char_pos(item, '>')+1:])
    elif find_char_pos(item, '<') != -1:
            left, op, right =  (item[0:find_char_pos(item, '<')]),"<",(item[find_char_pos(item, '<')+1:])
    elif find_char_pos(item, '>') != -1:
            side, left, op, right =  (item[0:find_char_pos(item, '>')]),">",(item[find_char_pos(item, '>')+1:])
    elif find_char_pos(item, '=') != -1:
            left, op, right =  (item[0:find_char_pos(item, '=')]),"=",(item[find_char_pos(item, '=')+1:])

    return (left, op, right)

def find_char_pos(string, char):
    if char in string:
        return string.find(char)
    else:
        return -1

if __name__ == "__main__":
    main()
