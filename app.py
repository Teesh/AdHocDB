import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *
import re
import time
from collections import defaultdict

conds = ["movies.movie_title == 'King Kong'","movies.actor_1_facebook_likes < 20000","actor_2_facebook_likes > actor_1_facebook_likes","movies.title_year < 2010"]

#movies = pandas.read_csv('./movies.csv')

#print(movies[eval(conds[1]) & eval(conds[3])])

path = "Datasets/Yelp/"

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

    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            select = parsed.tokens.index(item)
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_ind = parsed.tokens.index(item)
        elif item.ttype is None and isinstance(item, Where):
            where_ind = parsed.tokens.index(item)

    if (where_ind > 0):
        select_columns = get_select_names(parsed, select, from_ind)
        from_tables = get_table_names(parsed, from_ind, where_ind)
        where_condition = get_conditions(parsed, where_ind)
        where_columns = get_column(where_condition)
    else:
        select_columns = get_select_names(parsed, select, from_ind)
        from_tables = get_table_names(parsed, from_ind, len(parsed.tokens))


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


def query_plan(table_list, where_condition, select_columns, where_columns):
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
            globals()[rename] = eval('pandas.read_csv("' +path+ t + '.csv")['+cols+']')
            eval(rename+".rename(columns=lambda x: x+'__"+rename+"', inplace=True)")
            #read index files on columns

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
            # get index files on columns

    return eval_or(where_condition, select_columns)


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
        #if there is a not, then negate the condition
        left_cond = eval_not(left)
        #compbine table from right with left cond
        return combine_and(left_cond, right_table)
    else:
        #last condition, negate and then evaluate
        cond = eval_not(conditions)
        return eval_cond(cond)


def eval_not(condition):
    cond_lower = [c.lower() for c in condition]
    if 'not' in cond_lower:
        a = cond_lower.index('not')
        neg = negate(condition[a+1:])
        return neg
    else:
        return condition


def index_join(indl, indr):


def naive_join(l_table, r_table):
    pass

def index_search(col, value, binop):
    ids = []
    if 'stars' in col:
        #working with stars index
        #todo: ids = getIDs_stars(value)
        pass
    if 'city' in col:
        #todo: ids = getIDs_city(value)
        pass
    if 'state' in col:
        #todo: ids = getIDs_state(value)
        pass
    if 'name' in col:
        #todo: ids = getIDs_name(value)
        pass
    if 'postal_code' in col:
        #todo: ids = getIDs_postal(value)
        pass
    if 'label' in col:
        #todo: ids = getIDs_label(value)
        pass
    if 'funny' in col:
        #todo: ids = getIDs_funny(value, binop)
        pass
    if 'useful' in col:
        #todo: ids = getIDs_useful(value, binop)
        pass
    return ids

def get_rows_from_ids(ids, table, col):
    return eval("table.loc[table['"+col+"'].isin("+ids+")")
    #df.loc[df['B'].isin(['one', 'three'])]


def combine_and(left_cond, right_result):
    left,binop,right = comparision_parse(left_cond)
    left_left, arithm_op, left_right, binop, right = arithm_parse_eval(left,binop,right)
    ntable, left_table, right_table, left_col, right_col = check_num_table(left_left, right)

    if left_col not in right_result.columns:
        #completely disjoint
        #evaluate left
        left_result = eval_cond(left_cond)

        #cross join with right
        #todo: use naive join method
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
            #todo: change to index search
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
            #todo: change to naive join
            out = pandas.merge(right_result, rt, on='tmp')
            return eval("out.query('"+left_col + binop + right_col+"')")
            pass



def eval_cond(condition):
    result = []
    left,binop,right = comparision_parse(condition)
    left_left, arithm_op, left_right, binop, right = arithm_parse_eval(left,binop,right)
    ntable, left_table, right_table, left_col, right_col = check_num_table(left_left, right)
    if ntable == 1:
        #we can evaluate the condition here itself and return
        return eval(left_table + '.query("'+left_col + binop + right+ '")')
        #todo: change this to an index search
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
            #todo: naive join
            out = pandas.merge(tmp, tmpr, on='tmp')
            # out = eval(left_table+'.merge('+right_table+', left_on="'+left_col+'", right_on="'+right_col+'")')
            return eval("out.query('"+left_col + binop + right_col+"')")
        pass


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
            ise = find_char_pos(w, '=')
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
        dot = find_char_pos(item, '.')
        if dot != -1:
            t = item[0:dot]
            c = item[dot+1:]
            if t in dictionary:
                dictionary[t].add(c)
            else:
                dictionary[t].add(c)
    return dictionary

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


def comparision_parse(item):
    left = None
    op = None
    right = None
    item = item[0]
    if (find_char_pos(item, '<') != -1 and find_char_pos(item, '=') != -1):
            left, op, right = (item[0:find_char_pos(item, '<')]),"<=",(item[find_char_pos(item, '=')+1:])
    elif (find_char_pos(item, '>') != -1 and find_char_pos(item, '=') != -1):
            left, op, right =  (item[0:find_char_pos(item, '>')]),">=",(item[find_char_pos(item, '=')+1:])
    elif (find_char_pos(item, '<') != -1 and find_char_pos(item, '>') != -1):
            left, op, right =  (item[0:find_char_pos(item, '<')]),"<>",(item[find_char_pos(item, '>')+1:])
    elif find_char_pos(item, '<') != -1:
            left, op, right =  (item[0:find_char_pos(item, '<')]),"<",(item[find_char_pos(item, '<')+1:])
    elif find_char_pos(item, '>') != -1:
            left, op, right =  (item[0:find_char_pos(item, '>')]),">",(item[find_char_pos(item, '>')+1:])
    elif find_char_pos(item, '=') != -1:
            left, op, right =  (item[0:find_char_pos(item, '=')]),"==",(item[find_char_pos(item, '=')+1:])
    return (left, op, right)

def find_char_pos(string, char):
    if char in string:
        return string.find(char)
    else:
        return -1

def get_column_helper(item):
    if find_char_pos(item, '.') != -1:
        return(item[0:find_char_pos(item, '.')],item[find_char_pos(item, '.')+1:])
    return (None, None)

def get_column(condition):
    column = []
    dictionary = defaultdict(set)
    for item in condition:
        item = rm_white(item)
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
def rm_white(string):
    return string.replace(" ", "")
if __name__ == "__main__":
    main()
