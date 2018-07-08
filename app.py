import argparse
import sqlparse
from sqlparse.sql import *
from sqlparse.tokens import *

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

  print(select, from_ind, where_ind)

  select_columns = get_columns_or_tables(parsed, select, from_ind)
  from_tables = get_columns_or_tables(parsed, from_ind, where_ind)
  get_tables(from_tables)
  where_condition = get_conditions(parsed, where_ind)

  #make sure that all the attributes in the select and where
  #match with the attributes of the tables
  verify_attributes()

  #figure out how exactly to do the computations
  query_plan()



def get_tables(table_list):
  for table in table_list:
    #use pandas here to upload the tables into memory
    pass


def verify_attributes():
  pass


def query_plan():
  pass


def get_columns_or_tables(parsed, start, stop):
  token_stream = parsed.tokens[start:stop]
  print(token_stream)
  case = -1
  for item in token_stream:
    if isinstance(item, IdentifierList) or isinstance(item, Identifier): 
      print(item.value.split(','))
      return item.value.split(',')


def get_conditions(parsed, start):
  token_stream = parsed.tokens[start:]
  parsed_where = next(token for token in token_stream if isinstance(token, Where))[1:]
  # print(parsed_where)
  # print(parsed_where[1].value)
  conditions = []
  for token in parsed_where:
    if token.ttype is Keyword:
      conditions.append(token.value)
    if token.ttype is None:
      conditions.append(token.value)
  print(conditions)
  return conditions


if __name__ == "__main__":
    main()