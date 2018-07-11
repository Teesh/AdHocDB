import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *

#conds = ["movies.movie_title == 'King Kong'","movies.actor_1_facebook_likes < 20000","actor_2_facebook_likes > actor_1_facebook_likes","movies.title_year < 2010"]

#movies = pandas.read_csv('../Datasets/Movies/movies.csv')

#print(movies[eval(conds[1]) & eval(conds[3])])

def main():
  parser = argparse.ArgumentParser(description='Preprocessing files and running queries.')
  parser.add_argument('--preprocess', metavar = 'Files', nargs='+', type=str,
                      help='Preprocess files where files are given in a space seperated list. Ex. File1 File2 ...')
  parser.add_argument('--query', type=str,  nargs='+',
                      help='Run a query with the syntax SELECT <columns> FROM <tables> WHERE <condition>')

  args = parser.parse_args()
  query = args.query
  preprocess = args.preprocess
  if preprocess is None:
    #go to query function since no preprocessing needed
    query = ' '.join(query)
    print("here")
    parsing(query)
    
  if query is None:
    #go to preprocessing function
    print("here2")


def parsing(query):
  select = -1
  from_ind = -1
  where_ind = -1

  parsed = sqlparse.parse(query)[0]
  print(parsed.tokens)

  for item in parsed.tokens:
    print(item.ttype)
    if item.ttype is DML and item.value.upper() == 'SELECT':
      select = parsed.tokens.index(item)
    elif item.ttype is Keyword and item.value.upper() == 'FROM':
      from_ind = parsed.tokens.index(item)
    elif item.ttype is None and isinstance(item, Where):
      where_ind = parsed.tokens.index(item)

  print(select, from_ind, where_ind, len(parsed.tokens))

  if (where_ind > 0):
  	select_columns = get_columns_or_tables(parsed, select, from_ind)
  	from_tables = get_columns_or_tables(parsed, from_ind, where_ind)
  	get_tables(from_tables)
  	where_condition = get_conditions(parsed, where_ind)
  else:
  	select_columns = get_columns_or_tables(parsed, select, from_ind)
  	from_tables = get_columns_or_tables(parsed, from_ind, len(parsed.tokens))

  #make sure that all the attributes in the select and where
  #match with the attributes of the tables
  verify_attributes()

  #figure out how exactly to do the computations
  query_plan()

  #More to do...not sure what yet
  #print(movies[eval(where_condition[0])]); 


def get_tables(table_list):
  for table in table_list:
    #use pandas here to upload the tables into memory
    pass


def verify_attributes():
  pass


def query_plan():
  pass


def get_columns_or_tables(parsed, start, stop):
  '''
  Returns the names of the columns after the SELECT keyword or 
  the names of the tables after the FROM keyword

  Input: 
    parsed List: Parsed query tokens 
    start int: starting point of SELECT or FROM
    stop int: ending point of SELECT or From statement

  Returns: List of strings

  '''
  token_stream = parsed.tokens[start:stop]
  print(token_stream)
  for item in token_stream:
    if isinstance(item, IdentifierList) or isinstance(item, Identifier): 
      print(item.value.split(','))
      return item.value.split(',')


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
  print(conditions)
  return conditions


if __name__ == "__main__":
    main()
